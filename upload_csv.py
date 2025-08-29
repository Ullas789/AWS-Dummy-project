import os
from utils.constants import *
from utils.s3_utils import *
from utils.exceptions import *

s3=s3_config

try:
    s3.create_bucket(Bucket=BUCKET)
    print("Created bucket", BUCKET)
except Exception as e:
    print("Bucket create:", e)
    raise S3BucketCreationError("Failed to Create S3 Bucket") from e

# upload file
local_file=folder_path
key = os.path.basename(local_file)
with open(local_file, "rb") as f:
    s3.upload_fileobj(f, BUCKET, key)
    print(f"Uploaded {local_file} -> s3://{BUCKET}/{key}")
