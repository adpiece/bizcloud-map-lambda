import json
import os

from handlers.csv_export import lambda_handler


def main() -> None:
  default_payload = {
      "file_type": "users",
      "record_ids": [1, 2, 3],
      "is_all_record": False,
      "exported_file_id": 6,
  }

  payload_raw = os.environ.get("SQS_PAYLOAD")
  payload = json.loads(payload_raw) if payload_raw else default_payload
  event = {"Records": [{"body": json.dumps(payload)}]}

  print(lambda_handler(event, None))


if __name__ == "__main__":
  main()

