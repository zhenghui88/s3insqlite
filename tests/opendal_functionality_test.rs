mod common;
use opendal::Operator;
use opendal::services;
use rand::Rng;

#[tokio::test]
async fn test_connection() {
    // --- Configuration ---

    let (endpoint, bucket) = common::read_config();
    let access_key_id = "minioadmin";
    let secret_access_key = "minioadmin";
    let region = "auto";

    // --- Set up opendal S3 backend ---
    let builder = services::S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(access_key_id)
        .secret_access_key(secret_access_key)
        .region(region);

    let op = Operator::new(builder)
        .expect("failed to create S3 backend")
        .finish();

    let mut rng = rand::rng();
    // --- Generate random content and key ---
    let object_key = format!("test-object-{}.txt", rng.random::<char>());
    let random_content: String = rng
        .sample_iter(rand::distr::Alphabetic)
        .take(1024)
        .map(char::from)
        .collect();

    // --- Write (upload) the file ---
    op.write(&object_key, random_content.clone())
        .await
        .expect("failed to upload file");
    println!("Uploaded object: {object_key}");

    // --- Read (download) the file ---
    let downloaded_bytes = op.read(&object_key).await.expect("failed to download file");
    let downloaded_content = String::from_utf8(downloaded_bytes.to_vec())
        .expect("downloaded content is not valid UTF-8");

    // --- Validate content ---
    assert_eq!(
        random_content, downloaded_content,
        "Downloaded content does not match uploaded content"
    );
    println!("Content validated successfully!");

    // --- Delete the object ---
    op.delete(&object_key)
        .await
        .expect("failed to delete object");
    println!("Deleted object: {object_key}");
}
