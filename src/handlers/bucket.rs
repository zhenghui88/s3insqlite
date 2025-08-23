use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{AppState, ListBucketResult};
use crate::utils::{sanitize_bucket_name, validate_bucket, xml_error_response};

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
pub async fn bucket_dispatch(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    query: Query<HashMap<String, String>>,
) -> Response {
    // S3 ListObjectsV2: GET /?list-type=2
    if query.get("list-type").map(|v| v == "2").unwrap_or(false) {
        list_objects_v2(state, bucket, query.0).await
    } else if query.contains_key("versioning") {
        get_bucket_versioning(State(state), Path(bucket)).await
    } else {
        (StatusCode::NOT_IMPLEMENTED, "Not implemented").into_response()
    }
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

    let table_name = match sanitize_bucket_name(&bucket) {
        Some(t) => t,
        None => {
            return xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name: {}", bucket),
            );
        }
    };

    // Build SQL query for keys, size, last_modified and md5
    let mut stmt = match conn.prepare(&format!(
        "SELECT key, length(data), last_modified, md5 FROM {table_name} WHERE key LIKE ?1",
    )) {
        Ok(stmt) => stmt,
        Err(e) => {
            error!("SQL preparation error: {}", e);
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("SQL preparation error: {}", e),
            );
        }
    };

    let sql_params = rusqlite::params![format!("{prefix}%")];

    let mut rows_vec = Vec::new();
    let rows = stmt.query_map(sql_params, |row| {
        let key: String = row.get(0)?;
        let size: usize = row.get(1)?;
        let last_modified_secs: i64 = row.get(2)?;
        let md5_hash: Option<String> = row.get(3).ok();

        // Convert seconds timestamp to DateTime
        let last_modified =
            DateTime::<Utc>::from_timestamp(last_modified_secs, 0).unwrap_or(Utc::now());

        Ok((key, size, last_modified, md5_hash))
    });

    match rows {
        Ok(rows) => {
            for row in rows {
                match row {
                    Ok(data) => rows_vec.push(data),
                    Err(_) => continue,
                }
            }
        }
        Err(e) => {
            error!("SQL query error: {}", e);
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("SQL query error: {}", e),
            );
        }
    }

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

    let body = result.to_xml();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (StatusCode::OK, headers, body).into_response()
}
