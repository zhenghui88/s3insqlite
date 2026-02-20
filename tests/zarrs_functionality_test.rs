mod common;
use ndarray::Array2;
use opendal::Operator;
use opendal::services;
use std::sync::Arc;
use zarrs::array::codec::ZstdCodec;
use zarrs::array::{ArrayBuilder, ArraySubset, FillValue, ZARR_NAN_F32, data_type};
use zarrs::storage::AsyncReadableWritableListableStorage;
use zarrs_opendal::AsyncOpendalStore;

#[tokio::test]
async fn test_zarrs_s3_write_read() {
    // --- S3/Opendal configuration ---
    let (endpoint, bucket) = common::read_config();
    let access_key_id = "minioadmin";
    let secret_access_key = "minioadmin";
    let region = "us-east-1";

    // Set up opendal S3 backend
    let builder = services::S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(access_key_id)
        .secret_access_key(secret_access_key)
        .region(region);

    let operator = Operator::new(builder)
        .expect("failed to create S3 backend")
        .finish();

    let store: AsyncReadableWritableListableStorage =
        Arc::new(AsyncOpendalStore::new(operator.clone()));

    zarrs::group::GroupBuilder::new()
        .build(store.clone(), "/")
        .expect("Failed to create group")
        .async_store_metadata()
        .await
        .expect("Failed to write group metadata");

    // --- Prepare data ---
    let data: Array2<f32> =
        Array2::from_shape_vec((10, 10), (0..100).map(|x| x as f32).collect()).unwrap();

    let array = ArrayBuilder::new(
        data.shape().iter().map(|x| *x as u64).collect::<Vec<_>>(), // array shape
        [10, 10], // regular chunk shape (non-zero elements)
        data_type::float32(),
        FillValue::from(ZARR_NAN_F32),
    )
    .bytes_to_bytes_codecs(vec![Arc::new(ZstdCodec::new(3, true))])
    .build(store.clone(), "/array")
    .expect("failed to create array");

    array
        .async_store_metadata()
        .await
        .expect("Failed to store metadata");

    array
        .async_store_array_subset(
            &ArraySubset::new_with_shape(vec![10, 10]),
            data.as_slice().unwrap(),
        )
        .await
        .expect("Failed to write data to Zarr array");

    // --- Read back the array ---
    let read_elements = array
        .async_retrieve_array_subset::<Vec<f32>>(&array.subset_all())
        .await
        .expect("Failed to read data");

    let read_data =
        Array2::from_shape_vec((10, 10), read_elements).expect("Failed to convert to 2D array");

    // --- Validate ---
    assert_eq!(data, read_data, "Write-Read test failed: data mismatch");

    println!("Zarrs S3 write-read test passed!");
}
