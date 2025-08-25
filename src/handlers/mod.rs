pub mod bucket;
pub mod object;

// Re-exports for convenience
pub use bucket::{get_bucket_dispatch, list_buckets};
pub use object::{delete_object, download_object, head_object, upload_object};
