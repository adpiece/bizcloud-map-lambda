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
from config.csv_export.queries import get_query_builder

S3_CLIENT = boto3.client("s3")

USE_LOCAL_S3 = os.getenv("USE_LOCAL_S3", "").lower() in {"1", "true", "yes"}
LOCAL_S3_DIR = Path(os.getenv("LOCAL_S3_DIR", "/tmp/local_s3"))
LOCAL_S3_BASE_URL = os.getenv("LOCAL_S3_BASE_URL", "")


# --------------------------------------------------------------------------- #
# Data access helpers
# --------------------------------------------------------------------------- #

def _fetch_records(file_type: str, record_ids: List[int]) -> List[Dict[str, Any]]:
  """
  対象テーブルからレコードを取得する。

  Parameters
  ----------
  file_type: str
      ファイルタイプ（例: "users", "product"）
  record_ids: List[int]
      取得対象のIDリスト

  Returns
  -------
  List[Dict[str, Any]]
      取得したレコードのリスト
  """

  if not record_ids:
    raise ValueError("record_ids is required.")

  try:
    # file_typeに対応するクエリビルダーを取得
    build_query, transform_row = get_query_builder(file_type)
    query, params = build_query(record_ids)
  except KeyError:
    # クエリビルダーが存在しない場合は、デフォルトのクエリを使用
    table = file_type.lower()
    placeholders = ",".join(["%s"] * len(record_ids))
    query = f"SELECT * FROM {table} WHERE id IN ({placeholders})"
    params: List[Any] = record_ids
    
    # デフォルトの場合はtransform_rowは不要
    transform_row = lambda row: row

  with get_connection(timeout=5) as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, params)
      columns = [col[0] for col in cursor.description] if cursor.description else []
      rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
      # 各行を変換関数で処理
      return [transform_row(row) for row in rows]


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

  print("Lambda handler started")
  
  try:
    default_bucket = os.environ["EXPORT_CSV_BUCKET"]
    print(f"Bucket: {default_bucket}")
  except KeyError:
    print("ERROR: EXPORT_CSV_BUCKET environment variable is not set")
    return {
      "statusCode": 500,
      "body": json.dumps({"error": "EXPORT_CSV_BUCKET environment variable is required"}),
    }
  
  presigned_ttl = 7 * 24 * 3600
  results = []

  for record in event.get("Records", []):
    try:
      print("Processing record")
      payload = json.loads(record["body"])
      print(f"Payload: {json.dumps(payload)}")
      file_type = payload["file_type"]
      record_ids = payload.get("record_ids", [])
      if not record_ids:
        raise ValueError("record_ids is required.")
      exported_file_id = int(payload["exported_file_id"])
      table = file_type.lower()
      
      print(f"Fetching records from file_type: {file_type}, table: {table}, record_ids: {record_ids}")
      rows = _fetch_records(file_type, record_ids)
      print(f"Fetched {len(rows)} records")
      
      print("Generating CSV")
      csv_content = _to_csv(table, rows)
      print(f"CSV content length: {len(csv_content)}")

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
      print(f"Uploading to S3: {bucket}/{key}")

      _upload_to_s3(csv_content, bucket, key)
      print("S3 upload completed")
      
      print("Generating download URL")
      download_url = _generate_download_url(bucket, key, presigned_ttl)
      print(f"Download URL generated: {download_url}")
      
      print(f"Updating exported_file record: {exported_file_id}")
      _update_exported_file(exported_file_id, download_url)
      print("Database update completed")

      results.append(
          {
              "file_type": file_type,
              "exported_file_id": exported_file_id,
              "record_count": len(rows),
              "s3_url": download_url,
          }
      )

    except (KeyError, ValueError, json.JSONDecodeError) as exc:
      print(f"ERROR: invalid_message: {exc}")
      results.append(
          {
              "file_type": None,
              "exported_file_id": None,
              "error": f"invalid_message: {exc}",
          }
      )
    except DatabaseError as exc:
      print(f"ERROR: database_error: {exc}")
      results.append(
          {
              "file_type": payload.get("file_type"),
              "exported_file_id": payload.get("exported_file_id"),
              "error": f"database_error: {exc}",
          }
      )
    except Exception as exc:
      print(f"ERROR: unexpected_error: {exc}")
      import traceback
      print(f"Traceback: {traceback.format_exc()}")
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

