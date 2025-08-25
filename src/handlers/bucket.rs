use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{AppState, ListBucketResult};
use crate::utils::{bucket::query_bucket_objects, validate_bucket, xml_error_response};

/// S3 ListBuckets API: GET /
pub async fn list_buckets(
    State(state): State<Arc<AppState>>,
    query: Query<HashMap<String, String>>,
) -> Response {
    info!(
        "ListBuckets called, returning {} buckets",
        state.buckets.len()
    );

    let prefix = query.get("prefix");

    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("\n<ListAllMyBucketsResult>\n   <Buckets>");

    for bucket in state.buckets.iter() {
        if let Some(prefix) = prefix
            && !bucket.starts_with(prefix)
        {
            continue; // Skip buckets that don't match the prefix
        }
        xml.push_str(&format!("\n<Bucket>\n<Name>{bucket}</Name>\n</Bucket>"));
    }

    xml.push_str("\n</Buckets>");
    if let Some(prefix) = prefix {
        xml.push_str(&format!("\n<Prefix>{prefix}</Prefix>"));
    }
    xml.push_str("\n</ListAllMyBucketsResult>\n");

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", xml.len().to_string().parse().unwrap());

    (StatusCode::OK, headers, xml).into_response()
}

/// S3 Bucket Versioning endpoint
pub async fn get_bucket_versioning(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Response {
    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    info!("GetBucketVersioning for bucket '{bucket}'");

    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <VersioningConfiguration>
            <Status>Suspended</Status>
        </VersioningConfiguration>"#;

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", xml.len().to_string().parse().unwrap());

    (StatusCode::OK, headers, xml).into_response()
}

/// Route bucket operations based on query parameters
pub async fn get_bucket_dispatch(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    query: Query<HashMap<String, String>>,
) -> Response {
    if query.contains_key("versioning") {
        get_bucket_versioning(State(state), Path(bucket)).await
    } else if query.get("list-type").map(|v| v == "2").unwrap_or(false) {
        list_objects_v2(state, bucket, query.0).await
    } else {
        list_objects(state, bucket, query.0).await
    }
}

async fn list_objects(
    state: Arc<AppState>,
    bucket: String,
    params: HashMap<String, String>,
) -> Response {
    // Validate bucket
    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    // Extract query parameters for ListObjects v1
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params
        .get("delimiter")
        .and_then(|d| if d.is_empty() { None } else { d.chars().next() });
    let _marker = params.get("marker").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(i32::MAX); // disable limit by default

    // Get DB connection
    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            error!("Database connection error: {}", e);
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {}", e),
            );
        }
    };

    // Use shared query logic
    let rows_vec = match query_bucket_objects(&conn, &bucket, &prefix) {
        Ok(rows) => rows,
        Err(resp) => return *resp,
    };

    // Build ListBucketResult (v1 style)
    let mut result = ListBucketResult::new(&bucket, &prefix, delimiter);
    result.set_max_keys(max_keys);
    result.is_truncated = false; // disable pagination for now
    // v1: no encoding_type, no continuation_token, no start_after

    // Process the collected keys with md5 hashes
    result.process_keys(rows_vec);

    let body = result.to_xml();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (StatusCode::OK, headers, body).into_response()
}

/// Implementation for ListObjectsV2 S3 API
async fn list_objects_v2(
    state: Arc<AppState>,
    bucket: String,
    params: HashMap<String, String>,
) -> Response {
    // Validate bucket
    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    // Extract query parameters used by S3 ListObjectsV2
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let encoding_type = params.get("encoding-type").cloned();
    let max_keys = params
        .get("max-keys")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(i32::MAX);
    let start_after = params.get("start-after").cloned();
    let continuation_token = params.get("continuation-token").cloned();

    // S3 API expects delimiter to be a single character (usually '/')
    // Extract just the first character if delimiter is present
    let delimiter = params
        .get("delimiter")
        .and_then(|d| if d.is_empty() { None } else { d.chars().next() });

    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            error!("Database connection error: {}", e);
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {}", e),
            );
        }
    };

    // Use shared query logic
    let rows_vec = match query_bucket_objects(&conn, &bucket, &prefix) {
        Ok(rows) => rows,
        Err(resp) => return *resp,
    };

    // Create and populate result
    let mut result = ListBucketResult::new(&bucket, &prefix, delimiter);

    // Set additional S3 response fields
    result.set_encoding_type(encoding_type);
    result.set_max_keys(max_keys);
    result.set_start_after(start_after);
    result.set_continuation(continuation_token, None); // We don't implement pagination yet

    // Process the collected keys with md5 hashes
    result.process_keys(rows_vec);

    info!(
        "ListObjectsV2 result: bucket='{}', prefix='{}', delimiter={:?}, contents_count={}, prefixes_count={}",
        bucket,
        prefix,
        delimiter
            .map(|c| c.to_string())
            .unwrap_or_else(|| "none".to_string()),
        result.contents.len(),
        result.common_prefixes.len()
    );

    let body = result.to_xml_v2();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (StatusCode::OK, headers, body).into_response()
}
