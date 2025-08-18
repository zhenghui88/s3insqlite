use actix_web::{App, HttpServer, web};
use log::{info, warn};
use std::collections::HashSet;
use std::env;

mod handlers;
mod models;
mod utils;

use models::{AppConfig, AppState};

#[actix_web::main]
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
    let data = web::Data::new(AppState {
        db_pool: pool,
        buckets: buckets_set,
    });

    let max_object_size = config.get_max_object_size();
    let max_workers = config.get_max_workers();
    info!(
        "Server configuration: bind={}:{}, workers={}, max_object_size={}",
        config.bind_address, config.port, max_workers, max_object_size
    );

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::PayloadConfig::new(max_object_size))
            .app_data(data.clone())
            // S3 ListBuckets API: GET /
            .route("/", web::get().to(handlers::list_buckets))
            // Path-style endpoints: /{bucket}/{key:.*} and /{bucket}
            .route("/{bucket}", web::get().to(handlers::bucket_dispatch))
            .route("/{bucket}/{key:.*}", web::put().to(handlers::upload_object))
            .route(
                "/{bucket}/{key:.*}",
                web::get().to(handlers::download_object),
            )
            .route(
                "/{bucket}/{key:.*}",
                web::delete().to(handlers::delete_object),
            )
            .route("/{bucket}/{key:.*}", web::head().to(handlers::head_object))
    })
    .workers(max_workers)
    .bind((config.bind_address.as_str(), config.port))?;

    info!(
        "Server started successfully! Listening on {}:{}",
        config.bind_address, config.port
    );
    server.run().await
}
