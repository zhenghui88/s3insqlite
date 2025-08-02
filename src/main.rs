use actix_web::web::Bytes;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{error, info, warn};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::OpenOptions;
use std::io::Write;

struct AppState {
    db_pool: Pool<SqliteConnectionManager>,
    buckets: HashSet<String>, // The expected buckets
}

/// S3 ListBuckets API: GET /
async fn list_buckets(data: web::Data<AppState>, req: HttpRequest) -> HttpResponse {
    info!(
        "ListBuckets called, returning {} buckets",
        data.buckets.len()
    );
    let query = req.query_string();
    let params: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let prefix = params.get("prefix");

    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("\n<ListAllMyBucketsResult>\n   <Buckets>");
    for bucket in &data.buckets {
        if let Some(prefix) = prefix {
            if !bucket.starts_with(prefix) {
                continue; // Skip buckets that don't match the prefix
            }
        }
        xml.push_str(&format!("\n<Bucket>\n<Name>{bucket}</Name>\n</Bucket>"));
    }
    xml.push_str("\n</Buckets>");
    if let Some(prefix) = prefix {
        xml.push_str(&format!("\n<Prefix>{prefix}</Prefix>"));
    }
    xml.push_str("\n</ListAllMyBucketsResult>\n");
    HttpResponse::Ok().content_type("application/xml").body(xml)
}

/// Sanitize bucket name to be a valid SQLite table name.
/// Returns Some(table_name) if valid, None if invalid.
fn sanitize_bucket_name(bucket: &str) -> Option<String> {
    // Only allow alphanumeric, underscore, and dash
    if bucket.is_empty()
        || !bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    Some(format!("bucket_{bucket}"))
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    database_path: String,
    buckets: Vec<String>, // List of allowed buckets
    port: u16,
    bind_address: String,
    log_path: String,
    log_level: String,              // Add log_level field
    max_workers: Option<usize>,     // Optional for backward compatibility
    max_object_size: Option<usize>, // Maximum object size in bytes, default to 1 MB
}

impl AppConfig {
    fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;
        settings.try_deserialize()
    }
}

/// Extract and validate bucket from Host header.
/// Returns Ok(bucket) if valid and allowed, otherwise returns an error HttpResponse.
fn validate_bucket(bucket: &str, data: &AppState) -> Result<String, HttpResponse> {
    if data.buckets.contains(bucket) {
        Ok(bucket.to_string())
    } else {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
            <Code>AccessDenied</Code>
            <Message>Bucket access denied: {bucket}</Message>
            </Error>"#
        );
        Err(HttpResponse::Forbidden()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body))
    }
}

async fn upload_object(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: Bytes,
) -> impl Responder {
    let (bucket, key) = path.into_inner();
    let bucket = match validate_bucket(&bucket, &data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    info!("Uploading object '{key}' to bucket '{bucket}'");
    let pool = &data.db_pool;
    let conn = pool.get().unwrap();
    if let Some(table_name) = sanitize_bucket_name(&bucket) {
        let mut stmt = conn
            .prepare(&format!(
                "INSERT INTO {table_name} (key, data) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET data=excluded.data",
            ))
            .expect("Failed to prepare SQL statement");
        match stmt.execute(params![key, &body[..]]) {
            Ok(_) => {
                info!("Object '{key}' uploaded to bucket '{bucket}'");
                // S3: 200 OK, no body required
                HttpResponse::Ok().finish()
            }
            Err(e) => {
                error!("Failed to upload object '{key}' to bucket '{bucket}': {e}",);
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>InternalError</Code>
                        <Message>{e}</Message>
                    </Error>"#
                );
                HttpResponse::InternalServerError()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
        }
    } else {
        warn!("Invalid bucket name attempted: {bucket}");
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>InvalidBucketName</Code>
                <Message>Invalid bucket name attempted: {bucket}</Message>
            </Error>"#
        );
        HttpResponse::BadRequest()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body)
    }
}

async fn download_object(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (bucket, key) = path.into_inner();
    info!("Downloading object '{key}' from bucket '{bucket}'");
    let bucket = match validate_bucket(&bucket, &data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let pool = &data.db_pool;
    let conn = pool.get().unwrap();
    if let Some(table_name) = sanitize_bucket_name(&bucket) {
        let sql = format!("SELECT data FROM {table_name} WHERE key = ?1");
        match conn.query_row(&sql, params![key], |row| row.get::<_, Vec<u8>>(0)) {
            Ok(data) => HttpResponse::Ok()
                .content_type("application/octet-stream")
                .insert_header(("Content-Length", data.len().to_string()))
                .body(data),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>NoSuchKey</Code>
                        <Message>The object you requested does not exist: {key}</Message>
                    </Error>"#
                );
                HttpResponse::NotFound()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
            Err(e) => {
                error!("Failed to download object '{key}' from bucket '{bucket}': {e}",);
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>InternalError</Code>
                        <Message>{e}</Message>
                    </Error>"#
                );
                HttpResponse::InternalServerError()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
        }
    } else {
        warn!("Invalid bucket name attempted: {bucket}");
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>InvalidBucketName</Code>
                <Message>Invalid bucket name attempted: {bucket}</Message>
            </Error>"#
        );
        HttpResponse::BadRequest()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body)
    }
}

async fn delete_object(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (bucket, key) = path.into_inner();
    info!("Deleting object '{key}' from bucket '{bucket}'",);
    let bucket = match validate_bucket(&bucket, &data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let pool = &data.db_pool;
    let conn = pool.get().unwrap();
    if let Some(table_name) = sanitize_bucket_name(&bucket) {
        let sql = format!("DELETE FROM {table_name} WHERE key = ?1");
        match conn.execute(&sql, params![key]) {
            Ok(affected) => {
                if affected == 0 {
                    let body = format!(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>NoSuchKey</Code>
                            <Message>The object for deletion does not exist: {key}</Message>
                        </Error>"#
                    );
                    HttpResponse::NotFound()
                        .content_type("text/xml")
                        .insert_header(("Content-Length", body.len().to_string()))
                        .body(body)
                } else {
                    HttpResponse::NoContent().finish()
                }
            }
            Err(e) => {
                error!("Failed to delete object '{key}' from bucket '{bucket}': {e}",);
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>InternalError</Code>
                        <Message>{e}</Message>
                    </Error>"#
                );
                HttpResponse::InternalServerError()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
        }
    } else {
        warn!("Invalid bucket name attempted: {bucket}");
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>InvalidBucketName</Code>
                <Message>Invalid bucket name attempted: {bucket}</Message>
            </Error>"#
        );
        HttpResponse::BadRequest()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body)
    }
}

async fn head_object(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (bucket, key) = path.into_inner();
    let bucket = match validate_bucket(&bucket, &data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    info!("HEAD object '{key}' from bucket '{bucket}'",);
    let pool = &data.db_pool;
    let conn = pool.get().unwrap();
    if let Some(table_name) = sanitize_bucket_name(&bucket) {
        let sql = format!("SELECT LENGTH(data) FROM {table_name} WHERE key = ?1");
        match conn.query_row(&sql, params![key], |row| row.get::<_, Option<i64>>(0)) {
            Ok(Some(len)) => HttpResponse::Ok()
                .insert_header(("Content-Length", len.to_string()))
                .finish(),
            Ok(None) | Err(rusqlite::Error::QueryReturnedNoRows) => {
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>NoSuchKey</Code>
                        <Message>The object you requested does not exist: {key}</Message>
                    </Error>"#
                );
                HttpResponse::NotFound()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
            Err(e) => {
                error!("Failed to head object '{key}' from bucket '{bucket}': {e}",);
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
                    <Error>
                        <Code>InternalError</Code>
                        <Message>{e}</Message>
                    </Error>"#
                );
                HttpResponse::InternalServerError()
                    .content_type("text/xml")
                    .insert_header(("Content-Length", body.len().to_string()))
                    .body(body)
            }
        }
    } else {
        warn!("Invalid bucket name attempted: {bucket}");
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>InvalidBucketName</Code>
                <Message>Invalid bucket name attempted: {bucket}</Message>
            </Error>"#
        );
        HttpResponse::BadRequest()
            .content_type("text/xml")
            .insert_header(("Content-Length", body.len().to_string()))
            .body(body)
    }
}

async fn get_bucket_versioning(data: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let bucket = path.into_inner();
    let bucket = match validate_bucket(&bucket, &data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    info!("GetBucketVersioning for bucket '{bucket}'");
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <VersioningConfiguration>
            <Status>Suspended</Status>
        </VersioningConfiguration>"#;
    HttpResponse::Ok().content_type("application/xml").body(xml)
}

async fn bucket_dispatch(
    data: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let bucket = path.into_inner();
    let query = req.query_string();
    let params: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();

    // S3 ListObjectsV2: GET /?list-type=2
    if params.get("list-type").map(|v| v == "2").unwrap_or(false) {
        list_objects_v2(&data, &bucket, &params).await
    } else if params.contains_key("versioning") {
        get_bucket_versioning(data, web::Path::from(bucket)).await
    } else {
        HttpResponse::NotFound().finish()
    }
}

// Extracted ListObjectsV2 logic
async fn list_objects_v2(
    data: &web::Data<AppState>,
    bucket: &str,
    params: &HashMap<String, String>,
) -> HttpResponse {
    // Validate bucket
    let bucket = match validate_bucket(bucket, data) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let prefix = params
        .get("prefix")
        .cloned()
        .unwrap_or_default()
        .to_string();
    let delimiter = params.get("delimiter").cloned();
    let encoding_type = params.get("encoding-type").cloned();

    let pool = &data.db_pool;
    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            let body = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>InternalError</Code>
                    <Message>{e}</Message>
                </Error>"#
            );
            return HttpResponse::InternalServerError()
                .content_type("text/xml")
                .insert_header(("Content-Length", body.len().to_string()))
                .body(body);
        }
    };

    let table_name = match sanitize_bucket_name(&bucket) {
        Some(t) => t,
        None => {
            let body = "Invalid bucket name";
            return HttpResponse::BadRequest()
                .content_type("text/plain")
                .insert_header(("Content-Length", body.len().to_string()))
                .body(body);
        }
    };

    // Build SQL query for keys and last_modified
    let mut stmt = conn.prepare(&format!(
        "SELECT key, length(data), last_modified FROM {table_name} WHERE key LIKE ?1 ORDER BY key ASC",
    )).expect("invalid SQL statement");
    let sql_params: Vec<String> = vec![format!("{prefix}%")];

    let rows = match stmt.query_map(
        rusqlite::params_from_iter(sql_params.iter().map(|s| s as &dyn rusqlite::ToSql)),
        |row| {
            let key: String = row.get(0)?;
            let size: usize = row.get(1)?;
            let last_modified_str: String = row.get(2)?;
            let last_modified =
                match NaiveDateTime::parse_from_str(&last_modified_str, "%Y-%m-%d %H:%M:%S") {
                    Ok(dt) => DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc),
                    Err(_) => Utc::now(),
                };
            Ok((key, size, last_modified))
        },
    ) {
        Ok(r) => r,
        Err(e) => {
            let body = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>InternalError</Code>
                    <Message>{e}</Message>
                </Error>"#
            );
            return HttpResponse::InternalServerError()
                .content_type("text/xml")
                .insert_header(("Content-Length", body.len().to_string()))
                .body(body);
        }
    };

    let mut contents = Vec::new();
    let mut common_prefixes = HashSet::new();

    for row in rows {
        match row {
            Ok((key, size, last_modified)) => {
                match delimiter {
                    Some(ref d) if key.starts_with(&prefix) && key.len() > prefix.len() => {
                        // Check if the key contains the delimiter
                        if let Some(pos) = &key[prefix.len()..].find(d) {
                            // Extract common prefix
                            let common_prefix = &key[..prefix.len() + pos + d.len()];
                            common_prefixes.insert(common_prefix.to_string());
                        } else {
                            // Add to contents if no delimiter found
                            contents.push((key, size, last_modified));
                        }
                    }
                    _ => contents.push((key, size, last_modified)), // No delimiter or no match, add to contents
                };
            }
            Err(_) => continue,
        }
    }

    log::info!(
        "ListObjectsV2 result: bucket='{bucket}', prefix='{prefix}', delimiter='{delimiter:?}', contents={contents:?}, common_prefixes={common_prefixes:?}",
    );

    // XML response
    let mut xml = String::new();
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("\n<ListBucketResult>");
    xml.push_str("<IsTruncated>false</IsTruncated>");
    xml.push_str(&format!("<Name>{}</Name>", &bucket));
    xml.push_str(&format!("<Prefix>{}</Prefix>", &prefix));
    if let Some(ref d) = delimiter {
        xml.push_str(&format!("<Delimiter>{d}</Delimiter>"));
    }
    xml.push_str(&format!("<KeyCount>{}</KeyCount>", contents.len()));
    if let Some(ref enc) = encoding_type {
        xml.push_str(&format!("<EncodingType>{enc}</EncodingType>"));
    }
    for (key, size, last_modified) in &contents {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{key}</Key>"));
        xml.push_str(&format!("<Size>{size}</Size>"));
        xml.push_str(&format!(
            "<LastModified>{}</LastModified>",
            last_modified.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        ));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }
    for prefix in &common_prefixes {
        xml.push_str("<CommonPrefixes>");
        xml.push_str(&format!("<Prefix>{prefix}</Prefix>"));
        xml.push_str("</CommonPrefixes>");
    }
    xml.push_str("</ListBucketResult>");

    HttpResponse::Ok().content_type("application/xml").body(xml)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse command line argument for config file path
    let config_path = env::args().nth(1).unwrap_or("config.toml".to_string());

    // Read config file
    let config = AppConfig::from_file(&config_path)
        .unwrap_or_else(|_| panic!("Failed to read config file {config_path}"));

    // Parse log level from config
    let log_level = match config.log_level.parse::<log::LevelFilter>() {
        Ok(level) => level,
        Err(_) => log::LevelFilter::Debug,
    };

    // Setup logging to file
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.log_path)
        .expect("Failed to open log file");
    let log_file = std::sync::Mutex::new(log_file);
    let logger = env_logger::Builder::new()
        .format(move |buf, record| {
            let mut log_file = log_file.lock().unwrap();
            let log_line = format!(
                "{} [{}] - {}\n",
                chrono::Utc::now().to_rfc3339(),
                record.level(),
                record.args()
            );
            let _ = log_file.write_all(log_line.as_bytes());
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Utc::now().to_rfc3339(),
                record.level(),
                record.args()
            )
        })
        .filter_level(log_level)
        .build();
    log::set_boxed_logger(Box::new(logger)).unwrap();
    log::set_max_level(log_level);

    // Setup r2d2 connection pool
    let manager = SqliteConnectionManager::file(&config.database_path);
    let pool = Pool::new(manager).expect("Failed to create DB pool");
    // No global migrations needed; each bucket gets its own table.

    // Ensure all buckets from config exist in the database
    let mut buckets_set = HashSet::new();
    {
        let conn = pool.get().unwrap();
        for bucket in &config.buckets {
            if let Some(table_name) = sanitize_bucket_name(bucket) {
                let sql = format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (
                        key TEXT NOT NULL PRIMARY KEY,
                        data BLOB NOT NULL,
                        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )",
                );
                conn.execute(&sql, [])
                    .expect("Failed to create bucket table");
                let sql = format!(
                    "CREATE TRIGGER IF NOT EXISTS update_{table_name}_timestamp
                     AFTER UPDATE ON {table_name}
                     BEGIN UPDATE {table_name} SET last_modified = CURRENT_TIMESTAMP WHERE key = NEW.key; END;",
                );
                conn.execute(&sql, [])
                    .expect("Failed to create update trigger");
                buckets_set.insert(bucket.clone());
            } else {
                panic!("Invalid bucket name in config: {bucket}");
            }
        }
    }

    let data = web::Data::new(AppState {
        db_pool: pool,
        buckets: buckets_set,
    });

    let max_object_size = config.max_object_size.unwrap_or(1024 * 1024);
    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(web::PayloadConfig::new(max_object_size))
            .app_data(data.clone())
            // S3 ListBuckets API: GET /
            .route("/", web::get().to(list_buckets))
            // Path-style endpoints: /{bucket}/{key:.*} and /{bucket}
            .route("/{bucket}", web::get().to(bucket_dispatch))
            .route("/{bucket}/{key:.*}", web::put().to(upload_object))
            .route("/{bucket}/{key:.*}", web::get().to(download_object))
            .route("/{bucket}/{key:.*}", web::delete().to(delete_object))
            .route("/{bucket}/{key:.*}", web::head().to(head_object))
    })
    .bind((config.bind_address.as_str(), config.port))?;

    if let Some(workers) = config.max_workers {
        server = server.workers(workers);
    }

    server.run().await
}
