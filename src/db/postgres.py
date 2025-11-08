import os
import sys
from typing import Any

VENDOR_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "vendor"))
if VENDOR_PATH not in sys.path:
  sys.path.append(VENDOR_PATH)

import pg8000  # noqa: E402


DatabaseError = pg8000.Error


def get_connection(timeout: int = 5) -> Any:
  """
  PostgreSQLへの接続を作成して返す。

  Parameters
  ----------
  timeout: int
      接続タイムアウト秒数（デフォルト5秒）
  """

  return pg8000.connect(
      host=os.environ["DB_HOST"],
      port=int(os.environ.get("DB_PORT", "5432")),
      database=os.environ["DB_NAME"],
      user=os.environ["DB_USER"],
      password=os.environ["DB_PASSWORD"],
      timeout=timeout,
  )

