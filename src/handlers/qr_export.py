import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import boto3
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from PIL import Image

from db.postgres import DatabaseError, get_connection

S3_CLIENT = boto3.client("s3")

USE_LOCAL_S3 = os.getenv("USE_LOCAL_S3", "").lower() in {"1", "true", "yes"}
LOCAL_S3_DIR = Path(os.getenv("LOCAL_S3_DIR", "/var/task/.local_s3"))
LOCAL_S3_BASE_URL = os.getenv("LOCAL_S3_BASE_URL", "")

QR_LOGO_PATH = Path(os.getenv("QR_LOGO_PATH", "src/assets/minato_qr_logo.png"))
QR_LOGO_RATIO = float(os.getenv("QR_LOGO_RATIO", "0.25"))
QR_COLS_PER_ROW = int(os.getenv("QR_COLS_PER_ROW", "4"))


def _fetch_ids(table: str, record_ids: List[int], is_all_record: bool) -> List[int]:
  """
  対象テーブルから QR を発行するレコード ID を取得する。
  """

  query = f"SELECT id FROM {table}"
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
      return [row[0] for row in cursor.fetchall()]


def _build_qr_url(front_domain: str, record_id: int) -> str:
  """
  QR に埋め込む URL を生成する。
  例) https://example.com/quick_access/123
  """

  base = front_domain.rstrip("/")
  return f"{base}/quick_access/{record_id}"


def _generate_qr_image(data: str):
  """
  QR コード画像 (PIL.Image) を生成する。
  """

  qr = qrcode.QRCode(
      version=None,
      error_correction=qrcode.constants.ERROR_CORRECT_M,
      box_size=10,
      border=2,
  )
  qr.add_data(data)
  qr.make(fit=True)
  return qr.make_image(fill_color="black", back_color="white").convert("RGBA")


def _layout_qrs_to_pdf(image_data: Iterable[Dict[str, Any]], output_path: Path) -> None:
  """
  QR 画像を A4 PDF にレイアウトして保存する。

  環境変数 QR_COLS_PER_ROW で1行あたりのQR数を指定可能（4, 5, 6のいずれか）。
  デフォルトは4。
  """

  page_width, page_height = A4
  margin_x = 40
  margin_y = 40

  cols = QR_COLS_PER_ROW
  if cols not in {4, 5, 6, 7, 8}:
    cols = 4  # 無効な値の場合はデフォルトに戻す

  cell_width = (page_width - margin_x * 2) / cols
  # QR の下にラベルを置くため、1 セルの高さを「QR + ラベル余白」として少し大きめに確保
  label_height = 16
  cell_height = cell_width + label_height

  c = canvas.Canvas(str(output_path), pagesize=A4)

  col = 0
  row = 0
  logo_cache: Dict[str, ImageReader] = {}

  for item in image_data:
    img = item["image"]
    label = item.get("label")
    center_image = item.get("center_image")

    x = margin_x + col * cell_width
    # reportlab は左下が原点なので、上から下へ配置するように座標計算
    # セルの中で上側に QR、下側にラベルが来るようにオフセット
    y = page_height - margin_y - (row + 1) * cell_height + label_height

    # PIL.Image を一度一時ファイルに保存し、それを貼り付け
    # （メモリバッファでの貼り付けも可能だが、実装をシンプルに保つ）
    tmp_path = output_path.parent / f"._qr_tmp_{row}_{col}.png"
    img.resize((int(cell_width), int(cell_width))).save(tmp_path)
    c.drawImage(str(tmp_path), x, y, width=cell_width, height=cell_width)
    tmp_path.unlink(missing_ok=True)

    if center_image:
      try:
        if center_image not in logo_cache:
          logo_cache[center_image] = ImageReader(center_image)
        reader = logo_cache[center_image]
        img_w, img_h = reader.getSize()
      except Exception:
        reader = None
        img_w = img_h = 0

      if reader and img_w > 0:
        logo_width = cell_width * QR_LOGO_RATIO
        aspect = img_h / img_w
        logo_height = logo_width * aspect
        logo_x = x + (cell_width - logo_width) / 2
        logo_y = y + (cell_width - logo_height) / 2
        c.drawImage(reader, logo_x, logo_y, width=logo_width, height=logo_height, mask="auto")
        c.setStrokeColorRGB(0, 0, 0)
        c.setLineWidth(0.5)
        c.rect(logo_x, logo_y, logo_width, logo_height, fill=0, stroke=1)

    if label is not None:
      c.setFont("Helvetica", 8)
      # 中央揃えで QR のすぐ下に文字列を描画
      c.drawCentredString(x + cell_width / 2, y - 4, str(label))

    col += 1
    if col >= cols:
      col = 0
      row += 1
      if margin_y + (row + 1) * cell_height > page_height - margin_y:
        c.showPage()
        row = 0

  c.save()


# --------------------------------------------------------------------------- #
# S3 / Database helpers
# --------------------------------------------------------------------------- #

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


def _upload_to_s3(pdf_path: Path, bucket: str, key: str) -> None:
  """
  PDF ファイルを S3 にアップロードする。
  """

  if USE_LOCAL_S3:
    destination = LOCAL_S3_DIR / bucket / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(pdf_path.read_bytes())
    return

  S3_CLIENT.put_object(
      Bucket=bucket,
      Key=key,
      Body=pdf_path.read_bytes(),
      ContentType="application/pdf",
  )


def _build_s3_key(table: str, exported_file_id: int) -> str:
  """
  QR PDF 用の S3 キーを生成する。
  """

  timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
  return f"qr_exports/{table}/{exported_file_id}-{timestamp}.pdf"


def _generate_download_url(bucket: str, key: str, expires_in: int) -> str:
  """
  Presigned URL を生成する（ローカルモード時はファイルパスまたはカスタムURL）。
  """

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
# QR PDF generation
# --------------------------------------------------------------------------- #

def generate_qr_pdf(table: str, record_ids: List[int], is_all_record: bool) -> Path:
  """
  指定テーブル / レコード ID に対する QR コード PDF をローカルに出力する。

  Parameters
  ----------
  table: str
      対象テーブル名（小文字想定）
  record_ids: List[int]
      対象レコード ID のリスト
  is_all_record: bool
      True の場合、テーブル全件分の QR を作成する
  """

  front_domain = os.environ["FRONT_DOMAIN"]

  ids = _fetch_ids(table, record_ids, is_all_record)
  if not ids:
    raise ValueError("QR を作成する対象レコードが存在しません。")

  images: List[Dict[str, Any]] = []
  for record_id in ids:
    url = _build_qr_url(front_domain, record_id)
    img = _generate_qr_image(url)
    # ラベルはパス部分だけにして見栄えを良くする (例: /quick_access/123)
    label = f"/quick_access/{record_id}"
    center_image_path = str(QR_LOGO_PATH) if QR_LOGO_PATH.exists() else None
    images.append(
        {
            "image": img,
            "label": label,
            "center_image": center_image_path,
        }
    )

  # 一時ファイルに PDF を出力
  with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", prefix=f"{table}_qr_") as tmp_file:
    output_path = Path(tmp_file.name)

  _layout_qrs_to_pdf(images, output_path)
  return output_path


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  SQSメッセージに基づいてQR PDFを生成し、S3に配置し、exported_files を更新する。

  期待するメッセージ例:
  {
      "table": "users",
      "record_ids": [1, 2, 3],
      "is_all_record": false,
      "exported_file_id": 42
  }
  """

  default_bucket = os.getenv("EXPORT_QR_BUCKET")
  if not default_bucket:
    if USE_LOCAL_S3:
      default_bucket = os.getenv("LOCAL_S3_BUCKET", "s3-bucket")
    else:
      raise KeyError("EXPORT_QR_BUCKET environment variable is required")

  presigned_ttl = 7 * 24 * 3600
  results = []

  for record in event.get("Records", []):
    try:
      payload = json.loads(record["body"])
      table = payload["table"]
      record_ids = payload.get("record_ids", [])
      is_all_record = bool(payload.get("is_all_record", False))
      exported_file_id = int(payload["exported_file_id"])

      output_path = generate_qr_pdf(table, record_ids, is_all_record)

      try:
        bucket = default_bucket
        key = _build_s3_key(table, exported_file_id)

        _upload_to_s3(output_path, bucket, key)
        download_url = _generate_download_url(bucket, key, presigned_ttl)
        _update_exported_file(exported_file_id, download_url)

        results.append(
            {
                "table": table,
                "exported_file_id": exported_file_id,
                "record_count": len(record_ids) if not is_all_record else "all",
                "s3_url": download_url,
            }
        )
      finally:
        # 一時ファイルを削除
        if output_path.exists():
          output_path.unlink(missing_ok=True)

    except (KeyError, ValueError, json.JSONDecodeError) as exc:
      results.append(
          {
              "table": None,
              "exported_file_id": None,
              "error": f"invalid_message: {exc}",
          }
      )
    except DatabaseError as exc:
      results.append(
          {
              "table": payload.get("table"),
              "exported_file_id": payload.get("exported_file_id"),
              "error": f"database_error: {exc}",
          }
      )
    except Exception as exc:
      results.append(
          {
              "table": payload.get("table") if isinstance(payload, dict) else None,
              "exported_file_id": payload.get("exported_file_id") if isinstance(payload, dict) else None,
              "error": f"unexpected_error: {exc}",
          }
      )

  return {
      "statusCode": 200,
      "body": json.dumps({"results": results}),
  }


