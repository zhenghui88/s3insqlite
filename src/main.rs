use axum::{
    Router,
    routing::{delete, get, head, put},
};
use log::{error, info, warn};
use std::env;
use std::sync::Arc;
use std::{collections::HashSet, net::ToSocketAddrs};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

mod handlers;
mod models;
mod utils;

use models::{AppConfig, AppState};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Parse command line argument for config file path
    let config_path = env::args().nth(1).unwrap_or("config.toml".to_string());

    // Read config file
    let config = AppConfig::from_file(&config_path)
        .unwrap_or_else(|_| panic!("Failed to read config file {config_path}"));

    // Setup logging
    if let Err(e) = utils::initialize_logger(&config.log_path, &config.log_level) {
        eprintln!("Failed to initialize logger: {}", e);
        return Err(std::io::Error::other("Logger initialization failed"));
    }

    info!("Starting S3inSQLite server...");

    // Setup optimized connection pool
    let pool = utils::create_connection_pool(
        &config.database_path,
        config.get_db_pool_max_size(),
        config.get_db_pool_min_idle(),
        config.get_db_pool_timeout_seconds().as_secs(),
    )
    .expect("Failed to create database connection pool");

    // Ensure all buckets from config exist in the database
    let mut buckets_set = HashSet::new();
    {
        let conn = pool.get().unwrap();
        for bucket in &config.buckets {
            match utils::ensure_bucket_table(&conn, bucket) {
                Ok(_) => {
                    // Create indexes for better performance
                    if let Some(table_name) = utils::sanitize_bucket_name(bucket)
                        && let Err(e) = utils::create_bucket_indexes(&conn, &table_name)
                    {
                        warn!("Failed to create indexes for bucket {}: {}", bucket, e);
                    }
                    buckets_set.insert(bucket.clone());
                    info!("Initialized bucket: {}", bucket);
                }
                Err(e) => {
                    panic!("Failed to create bucket table for {}: {}", bucket, e);
                }
            }
        }
    }

    // Schedule periodic database optimization
    utils::schedule_optimization(pool.clone());

    // Create shared application state
    let state = Arc::new(AppState::new(pool, buckets_set));

    let max_object_size = config.get_max_object_size();
    let max_workers = config.get_max_workers();
    info!(
        "Server configuration: bind={}:{}, workers={}, max_object_size={}",
        config.bind_address, config.port, max_workers, max_object_size
    );

    // Build our application with the routes
    let app = Router::new()
        // S3 ListBuckets API: GET /
        .route("/", get(handlers::list_buckets))
        // Path-style endpoints: /{bucket}/{key:.*} and /{bucket}
        .route("/{bucket}", get(handlers::get_bucket_dispatch))
        .route("/{bucket}/", get(handlers::get_bucket_dispatch))
        .route("/{bucket}/{*key}", put(handlers::upload_object))
        .route("/{bucket}/{*key}", get(handlers::download_object))
        .route("/{bucket}/{*key}", delete(handlers::delete_object))
        .route("/{bucket}/{*key}", head(handlers::head_object))
        // Catch-all route for debugging unmatched requests
        .fallback(|req: axum::http::Request<axum::body::Body>| async move {
            use axum::{http::StatusCode, response::IntoResponse};
            let uri = req.uri().to_string();
            let method = req.method().to_string();
            error!("Fallback route hit for method: {} URI: {}", method, uri);
            (StatusCode::NOT_IMPLEMENTED, "").into_response()
        })
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .on_request(|req: &axum::http::Request<_>, _span: &tracing::Span| {
                    tracing::debug!(
                        "Incoming request: {} {}, headers: {:?}",
                        req.method(),
                        req.uri(),
                        req.headers()
                    );
                })
                .on_response(
                    |response: &axum::http::Response<_>,
                     _latency: std::time::Duration,
                     _span: &tracing::Span| {
                        tracing::debug!("Response: {:?}", response);
                    },
                ),
        );

    // Create socket address
    let addr = (config.bind_address.as_str(), config.port)
        .to_socket_addrs()
        .expect("Invalid socket address")
        .next()
        .unwrap();

    info!(
        "Server started successfully! Listening on {}:{}",
        config.bind_address, config.port
    );

    // Start the server
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}
