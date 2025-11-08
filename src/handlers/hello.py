import json
import os
from typing import Any, Dict

import pg8000


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


