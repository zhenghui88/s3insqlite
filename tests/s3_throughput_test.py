import boto3
from botocore.client import Config
import time
import os

# Configuration
endpoint_url = "http://localhost:9000"
aws_access_key_id = "rustfsadmin"
aws_secret_access_key = "rustfsadmin"
bucket_name = "test"
object_prefix = "benchblob"
object_size = 2 * 1024 * 1024
object_count = 1000


def random_bytes(size):
    # Use os.urandom for speed and randomness
    return os.urandom(size)


def benchmark_write(s3, bucket, prefix, size, count):
    print(
        f"Starting write benchmark: {count} objects of {size // (1024 * 1024)}MB each"
    )
    start = time.time()
    for i in range(count):
        key = f"{prefix}/{i:05d}.bin"
        data = random_bytes(size)
        s3.put_object(Bucket=bucket, Key=key, Body=data)
        if (i + 1) % 100 == 0:
            print(f"  Uploaded {i + 1}/{count} objects")
    end = time.time()
    elapsed = end - start
    mb_total = (size * count) / (1024 * 1024)
    print(f"Write benchmark finished in {elapsed:.2f} seconds")
    print(
        f"Write throughput: {mb_total / elapsed:.2f} MB/s, {count / elapsed:.2f} objects/s"
    )
    return elapsed


def benchmark_read(s3, bucket, prefix, size, count):
    print(f"Starting read benchmark: {count} objects of {size // (1024 * 1024)}MB each")
    start = time.time()
    for i in range(count):
        key = f"{prefix}/{i:05d}.bin"
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        assert len(data) == size, f"Read size mismatch for {key}"
        if (i + 1) % 100 == 0:
            print(f"  Downloaded {i + 1}/{count} objects")
    end = time.time()
    elapsed = end - start
    mb_total = (size * count) / (1024 * 1024)
    print(f"Read benchmark finished in {elapsed:.2f} seconds")
    print(
        f"Read throughput: {mb_total / elapsed:.2f} MB/s, {count / elapsed:.2f} objects/s"
    )
    return elapsed


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Ensure bucket exists
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in buckets:
        print(f"Bucket '{bucket_name}' does not exist. Creating...")
        s3.create_bucket(Bucket=bucket_name)

    # Write benchmark
    write_time = benchmark_write(
        s3, bucket_name, object_prefix, object_size, object_count
    )

    # Read benchmark
    read_time = benchmark_read(
        s3, bucket_name, object_prefix, object_size, object_count
    )

    print("\nBenchmark summary:")
    print(
        f"  Write: {object_count} objects x {object_size // (1024 * 1024)}MB in {write_time:.2f}s"
    )
    print(
        f"  Read:  {object_count} objects x {object_size // (1024 * 1024)}MB in {read_time:.2f}s"
    )


if __name__ == "__main__":
    main()
