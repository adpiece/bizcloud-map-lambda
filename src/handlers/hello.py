import json


def lambda_handler(event, context):
  return {
      "statusCode": 200,
      "headers": {
          "Content-Type": "application/json; charset=utf-8",
      },
      "body": json.dumps({"message": "Hello!!"}),
  }


