import json
import os

from handlers.qr_export import lambda_handler


def main() -> None:
  """
  QR PDF 生成処理をローカルから手軽に実行するためのランナー。

  - 環境変数 SQS_PAYLOAD に JSON を渡すと、その内容を payload として使用
  - 未設定の場合はデフォルト値で実行

  例:
    SQS_PAYLOAD='{"file_type": "users", "record_ids": [1,2,3], "exported_file_id": 42}'
  """

  default_payload = {
      "file_type": "users",
      "record_ids": [1, 2, 3],
      "exported_file_id": 1,
  }

  payload_raw = os.environ.get("SQS_PAYLOAD")
  payload = json.loads(payload_raw) if payload_raw else default_payload
  event = {"Records": [{"body": json.dumps(payload)}]}

  print(lambda_handler(event, None))


if __name__ == "__main__":
  main()


