import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

data=['input.csv']
folder_path = 'data/input.csv'
S3_LOCAL_TMP_DIR='/tmp'
BUCKET = os.getenv("S3_BUCKET", "my-bucket")

PG_HOST = os.getenv("PG_HOST", "localhost")   # 'localhost' when running on host; 'postgres' inside Docker network
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

DB_Connection= psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )