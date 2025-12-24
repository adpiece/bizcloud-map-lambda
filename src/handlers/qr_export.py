import json
import os
import tempfile
import traceback
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

# ロゴパスの解決: 環境変数が指定されていない場合、Lambda環境とローカル環境の両方に対応
_default_logo_paths = [
    "assets/minato_qr_logo.png",  # Lambda環境（/var/task/assets/）
    "src/assets/minato_qr_logo.png",  # ローカル環境
]
_logo_path = os.getenv("QR_LOGO_PATH")
if _logo_path:
    QR_LOGO_PATH = Path(_logo_path)
else:
    # デフォルトパスを順にチェック
    QR_LOGO_PATH = None
    for path_str in _default_logo_paths:
        candidate = Path(path_str)
        if candidate.exists():
            QR_LOGO_PATH = candidate
            break
    if QR_LOGO_PATH is None:
        # どちらも存在しない場合は最初のパスを使用（エラーハンドリングは後続処理で行う）
        QR_LOGO_PATH = Path(_default_logo_paths[0])

QR_LOGO_RATIO = float(os.getenv("QR_LOGO_RATIO", "0.25"))
QR_COLS_PER_ROW = int(os.getenv("QR_COLS_PER_ROW", "4"))


def _fetch_ids(table: str, record_ids: List[int]) -> List[int]:
  """
  対象テーブルから QR を発行するレコード ID を取得する。
  """

  if not record_ids:
    raise ValueError("record_ids is required.")

  placeholders = ",".join(["%s"] * len(record_ids))
  query = f"SELECT id FROM {table} WHERE id IN ({placeholders})"
  params: List[Any] = record_ids

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
          print(f"[PDF Layout] Loading logo image: {center_image}")
          logo_cache[center_image] = ImageReader(center_image)
        reader = logo_cache[center_image]
        img_w, img_h = reader.getSize()
      except Exception as e:
        print(f"[PDF Layout] WARNING: Failed to load logo image {center_image}: {str(e)}")
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
      elif center_image:
        print(f"[PDF Layout] WARNING: Logo image {center_image} could not be loaded (reader={reader}, size={img_w}x{img_h})")

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

  print(f"[DB Update] Updating exported_file: id={record_id}, url={download_url}")
  
  query = """
      UPDATE exported_files
      SET s3_url = %s,
          upload_status = %s,
          updated_at = NOW()
      WHERE id = %s
  """

  try:
    with get_connection(timeout=5) as conn:
      with conn.cursor() as cursor:
        cursor.execute(query, (download_url, 2, record_id))
        rows_affected = cursor.rowcount
        conn.commit()
        print(f"[DB Update] Success: id={record_id}, rows_affected={rows_affected}")
  except Exception as e:
    print(f"[DB Update] Error: id={record_id}, error={str(e)}")
    print(f"[DB Update] Traceback: {traceback.format_exc()}")
    raise


def _upload_to_s3(pdf_path: Path, bucket: str, key: str) -> None:
  """
  PDF ファイルを S3 にアップロードする。
  """

  print(f"[S3 Upload] Starting upload: bucket={bucket}, key={key}, file_size={pdf_path.stat().st_size} bytes")
  
  if USE_LOCAL_S3:
    destination = LOCAL_S3_DIR / bucket / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(pdf_path.read_bytes())
    print(f"[S3 Upload] Local S3 mode: File saved to {destination}")
    return

  try:
    response = S3_CLIENT.put_object(
        Bucket=bucket,
        Key=key,
        Body=pdf_path.read_bytes(),
        ContentType="application/pdf",
    )
    print(f"[S3 Upload] Success: bucket={bucket}, key={key}, etag={response.get('ETag', 'N/A')}")
  except Exception as e:
    print(f"[S3 Upload] Error: bucket={bucket}, key={key}, error={str(e)}")
    print(f"[S3 Upload] Traceback: {traceback.format_exc()}")
    raise


def _build_s3_key(file_type: str, exported_file_id: int) -> str:
  """
  QR PDF 用の S3 キーを生成する。
  """

  timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
  return f"qr_exports/{file_type}/{exported_file_id}-{timestamp}.pdf"


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

def generate_qr_pdf(table: str, record_ids: List[int]) -> Path:
  """
  指定テーブル / レコード ID に対する QR コード PDF をローカルに出力する。

  Parameters
  ----------
  table: str
      対象テーブル名（小文字想定）
  record_ids: List[int]
      対象レコード ID のリスト
  """

  front_domain = os.environ["FRONT_DOMAIN"]
  print(f"[PDF Generation] Front domain: {front_domain}")

  print(f"[PDF Generation] Fetching IDs: table={table}, record_ids={record_ids}")
  ids = _fetch_ids(table, record_ids)
  print(f"[PDF Generation] Fetched {len(ids)} record IDs: {ids}")
  
  if not ids:
    raise ValueError("QR を作成する対象レコードが存在しません。")

  print(f"[PDF Generation] Generating QR images for {len(ids)} records")
  # ロゴパスの確認とログ出力
  if QR_LOGO_PATH.exists():
    print(f"[PDF Generation] Logo image found: {QR_LOGO_PATH} (size: {QR_LOGO_PATH.stat().st_size} bytes)")
  else:
    print(f"[PDF Generation] WARNING: Logo image not found at {QR_LOGO_PATH}. QR codes will be generated without center logo.")
  
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

  print(f"[PDF Generation] Creating PDF layout with {len(images)} QR codes")
  _layout_qrs_to_pdf(images, output_path)
  print(f"[PDF Generation] PDF layout completed: {output_path}")
  return output_path


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  SQSメッセージに基づいてQR PDFを生成し、S3に配置し、exported_files を更新する。

  期待するメッセージ例:
  {
      "file_type": "users",
      "record_ids": [1, 2, 3],
      "exported_file_id": 42
  }
  """

  print(f"[Lambda Handler] Event received: {json.dumps(event, default=str)}")
  
  default_bucket = os.getenv("EXPORT_QR_BUCKET")
  if not default_bucket:
    if USE_LOCAL_S3:
      default_bucket = os.getenv("LOCAL_S3_BUCKET", "s3-bucket")
      print(f"[Lambda Handler] Using local S3 bucket: {default_bucket}")
    else:
      error_msg = "EXPORT_QR_BUCKET environment variable is required"
      print(f"[Lambda Handler] Error: {error_msg}")
      raise KeyError(error_msg)
  else:
    print(f"[Lambda Handler] Using S3 bucket: {default_bucket}")

  print(f"[Lambda Handler] USE_LOCAL_S3={USE_LOCAL_S3}")

  presigned_ttl = 7 * 24 * 3600
  results = []

  for record in event.get("Records", []):
    try:
      print(f"[Lambda Handler] Processing record: {record}")
      payload = json.loads(record["body"])
      file_type = payload["file_type"]
      record_ids = payload.get("record_ids", [])
      exported_file_id = int(payload["exported_file_id"])
      table = file_type.lower()

      print(f"[Lambda Handler] Parameters: file_type={file_type}, table={table}, record_ids={record_ids}, exported_file_id={exported_file_id}")

      print(f"[PDF Generation] Starting PDF generation for table={table}")
      output_path = generate_qr_pdf(table, record_ids)
      print(f"[PDF Generation] PDF generated: path={output_path}, size={output_path.stat().st_size} bytes")

      try:
        bucket = default_bucket
        key = _build_s3_key(file_type, exported_file_id)
        print(f"[S3] Prepared S3 location: bucket={bucket}, key={key}")

        _upload_to_s3(output_path, bucket, key)
        print(f"[S3] Upload completed successfully")
        
        download_url = _generate_download_url(bucket, key, presigned_ttl)
        print(f"[S3] Generated download URL: {download_url}")
        
        _update_exported_file(exported_file_id, download_url)
        print(f"[DB] Database update completed successfully")

        results.append(
            {
                "file_type": file_type,
                "exported_file_id": exported_file_id,
                "record_count": len(record_ids),
                "s3_url": download_url,
            }
        )
        print(f"[Lambda Handler] Successfully processed exported_file_id={exported_file_id}")
      finally:
        # 一時ファイルを削除
        if output_path.exists():
          output_path.unlink(missing_ok=True)
          print(f"[PDF Generation] Temporary file deleted: {output_path}")

    except (KeyError, ValueError, json.JSONDecodeError) as exc:
      error_msg = f"invalid_message: {exc}"
      print(f"[Lambda Handler] Error (invalid_message): {error_msg}")
      print(f"[Lambda Handler] Traceback: {traceback.format_exc()}")
      results.append(
          {
              "file_type": None,
              "exported_file_id": None,
              "error": error_msg,
          }
      )
    except DatabaseError as exc:
      error_msg = f"database_error: {exc}"
      print(f"[Lambda Handler] Error (database_error): {error_msg}")
      print(f"[Lambda Handler] Traceback: {traceback.format_exc()}")
      results.append(
          {
              "file_type": payload.get("file_type") if isinstance(payload, dict) else None,
              "exported_file_id": payload.get("exported_file_id") if isinstance(payload, dict) else None,
              "error": error_msg,
          }
      )
    except Exception as exc:
      error_msg = f"unexpected_error: {exc}"
      print(f"[Lambda Handler] Error (unexpected_error): {error_msg}")
      print(f"[Lambda Handler] Traceback: {traceback.format_exc()}")
      results.append(
          {
              "file_type": payload.get("file_type") if isinstance(payload, dict) else None,
              "exported_file_id": payload.get("exported_file_id") if isinstance(payload, dict) else None,
              "error": error_msg,
          }
      )

  response = {
      "statusCode": 200,
      "body": json.dumps({"results": results}),
  }
  print(f"[Lambda Handler] Returning response: {json.dumps(response, default=str)}")
  return response


