mod common;

use ndarray::Array2;
use opendal::Operator;
use opendal::services;
use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use zarrs::array::codec::ZstdCodec;
use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
use zarrs::storage::AsyncReadableWritableListableStorage;
use zarrs_opendal::AsyncOpendalStore;

const ACCESS_KEY_ID: &str = "minioadmin";
const SECRET_ACCESS_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const ARRAY_SHAPE: (usize, usize) = (128, 512); // 128x128 f32 array = 256KB per array
const ARRAY_COUNT: usize = 1000;

fn random_array(shape: (usize, usize)) -> Array2<f32> {
    let mut rng = rand::rng();
    Array2::from_shape_fn(shape, |_| rng.random())
}

async fn benchmark_write(
    store: &AsyncReadableWritableListableStorage,
    shape: (usize, usize),
    count: usize,
) -> f64 {
    println!("Starting Zarr write benchmark: {count} arrays of shape {shape:?}",);
    let start = Instant::now();
    for i in 0..count {
        let array_path = format!("/array_{i:05}");
        let data = random_array(shape);

        let array = ArrayBuilder::new(
            data.shape().iter().map(|x| *x as u64).collect(),
            DataType::Float32,
            vec![shape.0 as u64, shape.1 as u64].try_into().unwrap(),
            FillValue::from(ZARR_NAN_F32),
        )
        .bytes_to_bytes_codecs(vec![Arc::new(ZstdCodec::new(3, true))])
        .build(store.clone(), &array_path)
        .expect("failed to create array");

        array
            .async_store_metadata()
            .await
            .expect("Failed to store metadata");

        array
            .async_store_array_subset_ndarray(&[0, 0], data)
            .await
            .expect("Failed to write data to Zarr array");

        if (i + 1) % 100 == 0 {
            println!("  Uploaded {}/{} arrays", i + 1, count);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let array_bytes = (shape.0 * shape.1 * std::mem::size_of::<f32>()) as f64;
    let mb_total = (array_bytes * count as f64) / (1024.0 * 1024.0);
    println!("Write benchmark finished in {elapsed:.2} seconds");
    println!(
        "Write throughput: {:.2} MB/s, {:.2} arrays/s",
        mb_total / elapsed,
        count as f64 / elapsed
    );
    elapsed
}

async fn benchmark_read(
    store: &AsyncReadableWritableListableStorage,
    shape: (usize, usize),
    count: usize,
) -> f64 {
    println!("Starting Zarr read benchmark: {count} arrays of shape {shape:?}",);
    let start = Instant::now();
    for i in 0..count {
        let array_path = format!("/array_{i:05}");

        let array = zarrs::array::Array::async_open(store.clone(), &array_path)
            .await
            .expect("Failed to open array for reading");

        let read_data = array
            .async_retrieve_array_subset_ndarray::<f32>(&array.subset_all())
            .await
            .expect("Failed to read data");

        let read_data = read_data
            .into_dimensionality::<ndarray::Ix2>()
            .expect("Failed to convert to 2D array");

        assert_eq!(
            read_data.shape(),
            &[shape.0, shape.1],
            "Shape mismatch for {array_path}",
        );

        if (i + 1) % 100 == 0 {
            println!("  Downloaded {}/{} arrays", i + 1, count);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let array_bytes = (shape.0 * shape.1 * std::mem::size_of::<f32>()) as f64;
    let mb_total = (array_bytes * count as f64) / (1024.0 * 1024.0);
    println!("Read benchmark finished in {elapsed:.2} seconds");
    println!(
        "Read throughput: {:.2} MB/s, {:.2} arrays/s",
        mb_total / elapsed,
        count as f64 / elapsed
    );
    elapsed
}

#[tokio::test]
async fn zarrs_throughput_benchmark() {
    // Set up opendal S3 backend
    let (endpoint, bucket) = common::read_config();
    let builder = services::S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(ACCESS_KEY_ID)
        .secret_access_key(SECRET_ACCESS_KEY)
        .region(REGION);

    let operator = Operator::new(builder)
        .expect("failed to create S3 backend")
        .finish();

    let store: AsyncReadableWritableListableStorage =
        Arc::new(AsyncOpendalStore::new(operator.clone()));

    // Write benchmark
    let write_time = benchmark_write(&store, ARRAY_SHAPE, ARRAY_COUNT).await;

    // Read benchmark
    let read_time = benchmark_read(&store, ARRAY_SHAPE, ARRAY_COUNT).await;

    println!("\nBenchmark summary:");
    println!("  Write: {ARRAY_COUNT} arrays x {ARRAY_SHAPE:?} in {write_time:.2}s",);
    println!("  Read:  {ARRAY_COUNT} arrays x {ARRAY_SHAPE:?} in {read_time:.2}s",);
}
