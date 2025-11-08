import json
import os
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """PostgreSQLのusersテーブルから最初のレコードを取得する簡易テスト。"""

  db_host = os.environ["DB_HOST"]
  db_port = int(os.environ.get("DB_PORT", "5432"))
  db_name = os.environ["DB_NAME"]
  db_user = os.environ["DB_USER"]
  db_password = os.environ["DB_PASSWORD"]

  try:
    with psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        connect_timeout=5,
    ) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
      cursor.execute("SELECT * FROM users ORDER BY id ASC LIMIT 1;")
      row = cursor.fetchone()

      body = {
          "message": "success",
          "record": row,
      }

  except psycopg2.Error as exc:
    body = {
        "message": "database_error",
        "error": str(exc),
    }

  return {
      "statusCode": 200,
      "headers": {"Content-Type": "application/json; charset=utf-8"},
      "body": json.dumps(body, default=str),
  }


