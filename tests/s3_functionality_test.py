import random
import string
from pathlib import Path

import boto3
from botocore.client import Config

# Configuration
endpoint_url = "http://localhost:9000"  # Change if your API runs elsewhere
aws_access_key_id = "rustfsadmin"
aws_secret_access_key = "rustfsadmin"
bucket_name = "test"
object_key = "log.txt"
uploading_file = Path("uploading_file.txt")
downloaded_file = Path("downloaded_file.txt")

# Create random content and write to uploading_file.txt
random_content = "".join(random.choices(string.ascii_letters + string.digits, k=1024))
with open(uploading_file, "w") as f:
    f.write(random_content)

# Create S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# List buckets
print("Buckets:", s3.list_buckets())

# Upload the file
s3.upload_file(uploading_file, bucket_name, object_key)
print(f"Uploaded {uploading_file} to {bucket_name}/{object_key}")

# Download the file
s3.download_file(bucket_name, object_key, downloaded_file)
print(f"Downloaded {bucket_name}/{object_key} to {downloaded_file}")

# Verify file integrity
with open(uploading_file, "r") as f1, open(downloaded_file, "r") as f2:
    uploaded_content = f1.read()
    downloaded_content = f2.read()
    assert uploaded_content == downloaded_content, (
        "Downloaded file content does not match uploaded file!"
    )
print("File integrity verified: downloaded content matches uploaded content.")
uploading_file.unlink()
downloaded_file.unlink()


# List objects in the bucket
print("Objects:", s3.list_objects_v2(Bucket=bucket_name))
