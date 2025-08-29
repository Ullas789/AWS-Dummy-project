import os
# import boto3
import pandas as pd
# import psycopg2
from io import StringIO
from utils.constants import *
from utils.s3_utils import *
from utils.exceptions import *

def lambda_handler(event, context):
    s3 = s3_config

    # Extract bucket and file key from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    print(f"Reading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_content = obj["Body"].read().decode("utf-8")

    # Load into pandas
    df = pd.read_csv(StringIO(csv_content))

    # Data Cleaning Steps
    # 1. Drop rows where mandatory fields are empty
    mandatory_cols = ["id", "name", "age", "phone", "email"]
    df.dropna(subset=mandatory_cols, inplace=True)

    # 2. Correct data types
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    df.dropna(subset=["id", "age", "salary"], inplace=True)  # remove rows with wrong formats

    # 3. Remove duplicates
    df.drop_duplicates(inplace=True)

    # 4. Standardize text columns
    text_cols = ["name", "city", "email", "phone"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.title()  # capitalize words and remove extra spaces

    # Add processed column example
    df["processed_name"] = df["name"].str.upper()

    # Insert into Database
    conn = DB_Connection
    cursor = conn.cursor()

    # Drop table if it exists
    cursor.execute("DROP TABLE IF EXISTS processed_data;")

    # Create table
    cursor.execute("""
    CREATE TABLE processed_data (
        id SERIAL PRIMARY KEY,
        original_name TEXT,
        processed_name TEXT,
        age INT,
        city TEXT,
        phone TEXT,
        email TEXT,
        salary NUMERIC
    )
    """)

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO processed_data (original_name, processed_name, age, city, phone, email, salary)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (row["name"], row["processed_name"], row["age"], row["city"], row["phone"], row["email"], row["salary"])
        )

    conn.commit()
    cursor.close()
    conn.close()

    return {"status": "success", "rows": len(df)}
