from lambda_function import lambda_handler

event = {
    "Records": [
        {
            "s3": {
                "bucket": {"name": "my-bucket"},
                "object": {"key": "input.csv"}
            }
        }
    ]
}

if __name__ == "__main__":
    print(lambda_handler(event, None))
