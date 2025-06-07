mod common;
use opendal::Operator;
use opendal::services;
use rand::RngCore;
use std::time::Instant;

const ACCESS_KEY_ID: &str = "minioadmin";
const SECRET_ACCESS_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const OBJECT_PREFIX: &str = "benchblob";
const OBJECT_SIZE: usize = 256 * 1024;
const OBJECT_COUNT: usize = 1000;

fn random_bytes(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    rand::rng().fill_bytes(&mut buf);
    buf
}

async fn benchmark_write(op: &Operator, prefix: &str, size: usize, count: usize) -> f64 {
    println!(
        "Starting write benchmark: {} objects of {}KB each",
        count,
        size / 1024
    );
    let start = Instant::now();
    for i in 0..count {
        let key = format!("{}/{:05}.bin", prefix, i);
        let data = random_bytes(size);
        op.write(&key, data)
            .await
            .unwrap_or_else(|e| panic!("Failed to upload {}: {}", key, e));
        if (i + 1) % 100 == 0 {
            println!("  Uploaded {}/{} objects", i + 1, count);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let mb_total = (size * count) as f64 / (1024.0 * 1024.0);
    println!("Write benchmark finished in {:.2} seconds", elapsed);
    println!(
        "Write throughput: {:.2} MB/s, {:.2} objects/s",
        mb_total / elapsed,
        count as f64 / elapsed
    );
    elapsed
}

async fn benchmark_read(op: &Operator, prefix: &str, size: usize, count: usize) -> f64 {
    println!(
        "Starting read benchmark: {} objects of {}KB each",
        count,
        size / 1024
    );
    let start = Instant::now();
    for i in 0..count {
        let key = format!("{}/{:05}.bin", prefix, i);
        let data = op
            .read(&key)
            .await
            .unwrap_or_else(|e| panic!("Failed to download {}: {}", key, e));
        assert_eq!(
            data.len(),
            size,
            "Read size mismatch for {}: got {}, expected {}",
            key,
            data.len(),
            size
        );
        if (i + 1) % 100 == 0 {
            println!("  Downloaded {}/{} objects", i + 1, count);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let mb_total = (size * count) as f64 / (1024.0 * 1024.0);
    println!("Read benchmark finished in {:.2} seconds", elapsed);
    println!(
        "Read throughput: {:.2} MB/s, {:.2} objects/s",
        mb_total / elapsed,
        count as f64 / elapsed
    );
    elapsed
}

#[tokio::test]
async fn benchmark_throughput() {
    let (endpoint, bucket) = common::read_config();

    // Set up opendal S3 backend
    let builder = services::S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(ACCESS_KEY_ID)
        .secret_access_key(SECRET_ACCESS_KEY)
        .region(REGION);

    let op = Operator::new(builder)
        .expect("failed to create S3 backend")
        .finish();

    // Write benchmark
    let write_time = benchmark_write(&op, OBJECT_PREFIX, OBJECT_SIZE, OBJECT_COUNT).await;

    // Read benchmark
    let read_time = benchmark_read(&op, OBJECT_PREFIX, OBJECT_SIZE, OBJECT_COUNT).await;

    println!("\nBenchmark summary:");
    println!(
        "  Write: {} objects x {}KB in {:.2}s",
        OBJECT_COUNT,
        OBJECT_SIZE / 1024,
        write_time
    );
    println!(
        "  Read:  {} objects x {}KB in {:.2}s",
        OBJECT_COUNT,
        OBJECT_SIZE / 1024,
        read_time
    );
}
