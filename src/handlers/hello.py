import json
import os
import sys
from typing import Any, Dict

VENDOR_PATH = os.path.join(os.path.dirname(__file__), "..", "vendor")
if VENDOR_PATH not in sys.path:
  sys.path.append(VENDOR_PATH)

import pg8000  # noqa: E402


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """PostgreSQLのusersテーブルから最初のレコードを取得する簡易テスト。"""

  db_host = os.environ["DB_HOST"]
  db_port = int(os.environ.get("DB_PORT", "5432"))
  db_name = os.environ["DB_NAME"]
  db_user = os.environ["DB_USER"]
  db_password = os.environ["DB_PASSWORD"]

  try:
    conn = pg8000.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
        timeout=5,
    )
    try:
      with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users ORDER BY id ASC LIMIT 1;")
        columns = [col[0] for col in cursor.description] if cursor.description else []
        row = cursor.fetchone()
        record = dict(zip(columns, row)) if row and columns else None

      body = {
          "message": "success",
          "record": record,
      }
    finally:
      conn.close()

  except pg8000.Error as exc:
    body = {
        "message": "database_error",
        "error": str(exc),
    }

  return {
      "statusCode": 200,
      "headers": {"Content-Type": "application/json; charset=utf-8"},
      "body": json.dumps(body, default=str),
  }


