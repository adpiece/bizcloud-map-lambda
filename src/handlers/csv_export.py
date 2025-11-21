import csv
import io
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import boto3

from db.postgres import DatabaseError, get_connection
from config.csv_export.headers import TABLE_EXPORT_CONFIG

S3_CLIENT = boto3.client("s3")

USE_LOCAL_S3 = os.getenv("USE_LOCAL_S3", "").lower() in {"1", "true", "yes"}
LOCAL_S3_DIR = Path(os.getenv("LOCAL_S3_DIR", "/tmp/local_s3"))
LOCAL_S3_BASE_URL = os.getenv("LOCAL_S3_BASE_URL", "")


# --------------------------------------------------------------------------- #
# Data access helpers
# --------------------------------------------------------------------------- #

def _fetch_records(table: str, record_ids: List[int], is_all_record: bool) -> List[Dict[str, Any]]:
  """
  対象テーブルからレコードを取得する。

  Parameters
  ----------
  table: str
      取得対象のテーブル名（小文字想定）
  record_ids: List[int]
      取得対象のIDリスト
  is_all_record: bool
      True の場合、ID指定を無視して全件取得する
  """

  query = f"SELECT * FROM {table}"
  params: List[Any] = []

  if not is_all_record:
    if not record_ids:
      return []
    placeholders = ",".join(["%s"] * len(record_ids))
    query += f" WHERE id IN ({placeholders})"
    params = record_ids

  with get_connection(timeout=5) as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, params)
      columns = [col[0] for col in cursor.description] if cursor.description else []
      return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _update_exported_file(record_id: int, download_url: str) -> None:
  """
  exported_files テーブルを更新する。
  """

  query = """
      UPDATE exported_files
      SET s3_url = %s,
          upload_status = %s,
          updated_at = NOW()
      WHERE id = %s
  """

  with get_connection(timeout=5) as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, (download_url, 2, record_id))
      conn.commit()


# --------------------------------------------------------------------------- #
# CSV / S3 helpers
# --------------------------------------------------------------------------- #

def _to_csv(table: str, rows: Iterable[Dict[str, Any]]) -> str:
  rows = list(rows)
  if not rows:
    return ""

  config = TABLE_EXPORT_CONFIG.get(table, {})
  field_order = config.get("field_order")
  labels_map = config.get("labels", {})

  if field_order:
    # 余分なカラムを除外し、指定順に並べ替え
    reordered_rows = []
    for row in rows:
      reordered_rows.append({field: row.get(field) for field in field_order})
    rows = reordered_rows
    fieldnames = field_order
  else:
    fieldnames = list(rows[0].keys())

  header_row = {field: labels_map.get(field, field) for field in fieldnames}

  output = io.StringIO()
  writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
  writer.writerow(header_row)
  writer.writerows(rows)
  return output.getvalue()


def _upload_to_s3(csv_content: str, bucket: str, key: str) -> None:
  if USE_LOCAL_S3:
    destination = LOCAL_S3_DIR / bucket / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(csv_content, encoding="utf-8")
    return

  S3_CLIENT.put_object(
      Bucket=bucket,
      Key=key,
      Body=csv_content.encode("utf-8"),
      ContentType="text/csv; charset=utf-8",
  )


def _build_s3_key(file_type: str, exported_file_id: int) -> str:
  timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
  return f"csv_exports/{file_type}/{exported_file_id}-{timestamp}.csv"


def _generate_download_url(bucket: str, key: str, expires_in: int) -> str:
  if USE_LOCAL_S3:
    if LOCAL_S3_BASE_URL:
      base = LOCAL_S3_BASE_URL.rstrip("/")
      return f"{base}/{bucket}/{key}"
    return str((LOCAL_S3_DIR / bucket / key).resolve())

  return S3_CLIENT.generate_presigned_url(
      "get_object",
      Params={"Bucket": bucket, "Key": key},
      ExpiresIn=expires_in,
  )


# --------------------------------------------------------------------------- #
# Lambda handler
# --------------------------------------------------------------------------- #

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  SQSメッセージに基づいてCSVを生成し、S3に配置し、exported_files を更新する。
  Parametersは"debug/csv_export_runner.py"参照
  """

  default_bucket = os.environ["EXPORT_CSV_BUCKET"]
  presigned_ttl = 7 * 24 * 3600
  results = []

  for record in event.get("Records", []):
    try:
      payload = json.loads(record["body"])
      file_type = payload["file_type"]
      record_ids = payload.get("record_ids", [])
      is_all_record = bool(payload.get("is_all_record", False))
      exported_file_id = int(payload["exported_file_id"])
      table = file_type.lower()
      rows = _fetch_records(table, record_ids, is_all_record)
      csv_content = _to_csv(table, rows)

      if not csv_content:
        results.append(
            {
                "file_type": file_type,
                "exported_file_id": exported_file_id,
                "record_count": 0,
                "s3_url": None,
                "message": "no_records",
            }
        )
        continue

      bucket = default_bucket
      key = _build_s3_key(file_type, exported_file_id)

      _upload_to_s3(csv_content, bucket, key)
      download_url = _generate_download_url(bucket, key, presigned_ttl)
      _update_exported_file(exported_file_id, download_url)

      results.append(
          {
              "file_type": file_type,
              "exported_file_id": exported_file_id,
              "record_count": len(rows),
              "s3_url": download_url,
          }
      )

    except (KeyError, ValueError, json.JSONDecodeError) as exc:
      results.append(
          {
              "file_type": None,
              "exported_file_id": None,
              "error": f"invalid_message: {exc}",
          }
      )
    except DatabaseError as exc:
      results.append(
          {
              "file_type": payload.get("file_type"),
              "exported_file_id": payload.get("exported_file_id"),
              "error": f"database_error: {exc}",
          }
      )
    except Exception as exc:
      results.append(
          {
              "file_type": payload.get("file_type") if isinstance(payload, dict) else None,
              "exported_file_id": payload.get("exported_file_id") if isinstance(payload, dict) else None,
              "error": f"unexpected_error: {exc}",
          }
      )

  return {
      "statusCode": 200,
      "body": json.dumps({"results": results}),
  }

