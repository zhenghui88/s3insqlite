use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use rusqlite::Connection;
use std::fmt::Write;

/// Sanitize bucket name to be a valid SQLite table name.
/// Returns Some(table_name) if valid, None if invalid.
pub fn sanitize_bucket_name(bucket: &str) -> Option<String> {
    // Only allow alphanumeric, underscore, and dash
    if bucket.is_empty()
        || !bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    // replace dash with underscore
    let table_name = bucket.replace('-', "_");
    Some(format!("bucket_{table_name}"))
}

/// Extract and validate bucket name against allowed buckets.
/// Returns Ok(bucket) if valid and allowed, otherwise returns an S3 formatted error response.
pub fn validate_bucket(
    bucket: &str,
    allowed_buckets: &std::collections::HashSet<String>,
) -> Result<String, Box<Response>> {
    if allowed_buckets.contains(bucket) {
        Ok(bucket.to_string())
    } else {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
            <Code>AccessDenied</Code>
            <Message>Bucket access denied: {bucket}</Message>
            </Error>"#
        );

        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/xml".parse().unwrap());
        headers.insert("Content-Length", body.len().to_string().parse().unwrap());

        Err(Box::new(
            (StatusCode::FORBIDDEN, headers, body).into_response(),
        ))
    }
}

/// Query objects in a bucket with a prefix, returns Vec<(key, size, last_modified, md5)>
type QueryBucketResult = Vec<(String, usize, chrono::DateTime<chrono::Utc>, Option<String>)>;

pub fn query_bucket_objects(
    conn: &rusqlite::Connection,
    bucket: &str,
    prefix: &str,
) -> Result<QueryBucketResult, Box<Response>> {
    let table_name = match sanitize_bucket_name(bucket) {
        Some(t) => t,
        None => {
            return Err(Box::new(xml_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                &format!("Invalid bucket name: {}", bucket),
            )));
        }
    };

    let mut stmt = match conn.prepare(&format!(
        "SELECT key, length(data), last_modified, md5 FROM {table_name} WHERE key LIKE ?1",
    )) {
        Ok(stmt) => stmt,
        Err(e) => {
            return Err(Box::new(xml_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("SQL preparation error: {}", e),
            )));
        }
    };

    let sql_params = rusqlite::params![format!("{prefix}%")];

    let mut rows_vec = Vec::new();
    let rows = stmt.query_map(sql_params, |row| {
        let key: String = row.get(0)?;
        let size: usize = row.get(1)?;
        let last_modified_secs: i64 = row.get(2)?;
        let md5_hash: Option<String> = row.get(3).ok();

        let last_modified = chrono::DateTime::<chrono::Utc>::from_timestamp(last_modified_secs, 0)
            .unwrap_or(chrono::Utc::now());

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
            Ok(rows_vec)
        }
        Err(e) => Err(Box::new(xml_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("SQL query error: {}", e),
        ))),
    }
}

/// Ensures the bucket table exists in the database
pub fn ensure_bucket_table(conn: &Connection, bucket: &str) -> rusqlite::Result<()> {
    if let Some(table_name) = sanitize_bucket_name(bucket) {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                key TEXT NOT NULL PRIMARY KEY,
                data BLOB NOT NULL,
                last_modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                md5 TEXT(32) NOT NULL
            )",
        );
        conn.execute(&sql, [])?;

        let sql = format!(
            "CREATE TRIGGER IF NOT EXISTS update_{table_name}_timestamp
             AFTER UPDATE ON {table_name}
             BEGIN UPDATE {table_name} SET last_modified = strftime('%s', 'now') WHERE key = NEW.key; END;",
        );
        conn.execute(&sql, [])?;
        Ok(())
    } else {
        Err(rusqlite::Error::InvalidParameterName(format!(
            "Invalid bucket name: {bucket}"
        )))
    }
}

/// Generate S3 XML error response
pub fn generate_xml_error(code: &str, message: &str) -> String {
    let mut xml = String::new();
    write!(
        xml,
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>{}</Code>
            <Message>{}</Message>
        </Error>"#,
        code, message
    )
    .expect("Error formatting XML");
    xml
}

/// Generate HTTP response with XML error
pub fn xml_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let body = generate_xml_error(code, message);
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/xml".parse().unwrap());
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (status, headers, body).into_response()
}
