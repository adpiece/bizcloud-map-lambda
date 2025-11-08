import json
from typing import Any, Dict

from db.postgres import DatabaseError, get_connection


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """PostgreSQLのusersテーブルから最初のレコードを取得する簡易テスト。"""

  try:
    with get_connection(timeout=5) as conn:
      with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users ORDER BY id ASC LIMIT 1;")
        columns = [col[0] for col in cursor.description] if cursor.description else []
        row = cursor.fetchone()
        record = dict(zip(columns, row)) if row and columns else None

    body = {
        "message": "success",
        "record": record,
    }

  except DatabaseError as exc:
    body = {
        "message": "database_error",
        "error": str(exc),
    }

  return {
      "statusCode": 200,
      "headers": {"Content-Type": "application/json; charset=utf-8"},
      "body": json.dumps(body, default=str),
  }


