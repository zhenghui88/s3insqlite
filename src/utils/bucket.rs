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
) -> Result<String, actix_web::HttpResponse> {
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
        Err(actix_web::HttpResponse::Forbidden()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body))
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
pub fn xml_error_response(
    status: actix_web::http::StatusCode,
    code: &str,
    message: &str,
) -> actix_web::HttpResponse {
    let body = generate_xml_error(code, message);
    actix_web::HttpResponse::build(status)
        .content_type("text/xml")
        .insert_header(("Content-Length", body.len().to_string()))
        .body(body)
}
