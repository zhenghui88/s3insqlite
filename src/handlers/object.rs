use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use rusqlite::params;
use std::sync::Arc;

use crate::models::AppState;
use crate::utils::{sanitize_bucket_name, validate_bucket, xml_error_response};

/// Upload an object to a bucket
/// PUT /{bucket}/{key}
pub async fn upload_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    info!("Uploading object '{key}' to bucket '{bucket}'");
    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get database connection: {e}");
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {e}"),
            );
        }
    };

    match sanitize_bucket_name(&bucket) {
        Some(table_name) => {
            // Calculate MD5 hash of the data
            let md5_hash = hex::encode(md5::compute(&body[..]).0);

            let sql = format!(
                "INSERT INTO {table_name} (key, data, md5) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET data=excluded.data, md5=excluded.md5",
            );

            match conn.prepare(&sql) {
                Ok(mut stmt) => {
                    match stmt.execute(params![key, &body[..], md5_hash]) {
                        Ok(_) => {
                            info!("Uploaded object '{key}' to bucket '{bucket}'");
                            // S3: 200 OK, no body required
                            StatusCode::OK.into_response()
                        }
                        Err(e) => {
                            error!("Failed to upload object '{key}' to bucket '{bucket}': {e}");
                            xml_error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "InternalError",
                                &e.to_string(),
                            )
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to prepare statement: {e}");
                    xml_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        &e.to_string(),
                    )
                }
            }
        }
        None => {
            warn!("Invalid bucket name attempted: {bucket}");
            xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name attempted: {bucket}"),
            )
        }
    }
}

/// Download an object from a bucket
/// GET /{bucket}/{key}
pub async fn download_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    info!("Downloading object '{key}' from bucket '{bucket}'");

    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get database connection: {e}");
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {e}"),
            );
        }
    };

    match sanitize_bucket_name(&bucket) {
        Some(table_name) => {
            let sql = format!("SELECT data FROM {table_name} WHERE key = ?1");
            match conn.query_row(&sql, params![key], |row| row.get::<_, Vec<u8>>(0)) {
                Ok(data) => {
                    info!("Downloaded object '{key}' from bucket '{bucket}'");
                    let mut headers = HeaderMap::new();
                    headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                    headers.insert("Content-Length", data.len().to_string().parse().unwrap());

                    (StatusCode::OK, headers, data).into_response()
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => xml_error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    &format!("The object you requested does not exist: {key}"),
                ),
                Err(e) => {
                    error!("Failed to download object '{key}' from bucket '{bucket}': {e}");
                    xml_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        &e.to_string(),
                    )
                }
            }
        }
        None => {
            warn!("Invalid bucket name attempted: {bucket}");
            xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name attempted: {bucket}"),
            )
        }
    }
}

/// Delete an object from a bucket
/// DELETE /{bucket}/{key}
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    info!("Deleting object '{key}' from bucket '{bucket}'");

    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get database connection: {e}");
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {e}"),
            );
        }
    };

    match sanitize_bucket_name(&bucket) {
        Some(table_name) => {
            let sql = format!("DELETE FROM {table_name} WHERE key = ?1");
            match conn.execute(&sql, params![key]) {
                Ok(_) => {
                    info!("Deleted object '{key}' from bucket '{bucket}'");
                    StatusCode::NO_CONTENT.into_response()
                }
                Err(e) => {
                    error!("Failed to delete object '{key}' from bucket '{bucket}': {e}");
                    xml_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        &e.to_string(),
                    )
                }
            }
        }
        None => {
            warn!("Invalid bucket name attempted: {bucket}");
            xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name attempted: {bucket}"),
            )
        }
    }
}

/// Get object metadata without returning the object data
/// HEAD /{bucket}/{key}
pub async fn head_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let bucket = match validate_bucket(&bucket, &state.buckets) {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    info!("HEAD object '{key}' from bucket '{bucket}'");
    let pool = &state.db_pool;
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get database connection: {e}");
            return xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Database connection error: {e}"),
            );
        }
    };

    match sanitize_bucket_name(&bucket) {
        Some(table_name) => {
            let sql =
                format!("SELECT LENGTH(data), last_modified, md5 FROM {table_name} WHERE key = ?1");
            match conn.query_row(&sql, params![key], |row| {
                let size: i64 = row.get(0)?;
                let last_modified: i64 = row.get(1)?;
                let md5_hash: String = row.get(2)?;
                Ok((size, last_modified, md5_hash))
            }) {
                Ok((size, last_modified, md5_hash)) => {
                    // Convert seconds timestamp to DateTime
                    let last_modified_datetime =
                        DateTime::<Utc>::from_timestamp(last_modified, 0).unwrap_or(Utc::now());

                    let mut headers = HeaderMap::new();
                    headers.insert("Content-Length", size.to_string().parse().unwrap());
                    headers.insert(
                        "Last-Modified",
                        last_modified_datetime.to_rfc2822().parse().unwrap(),
                    );
                    headers.insert("ETag", format!("\"{}\"", md5_hash).parse().unwrap());

                    (StatusCode::OK, headers).into_response()
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => xml_error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    &format!("The object you requested does not exist: {key}"),
                ),
                Err(e) => {
                    error!("Failed to head object '{key}' from bucket '{bucket}': {e}");
                    xml_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        &e.to_string(),
                    )
                }
            }
        }
        None => {
            warn!("Invalid bucket name attempted: {bucket}");
            xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name attempted: {bucket}"),
            )
        }
    }
}
