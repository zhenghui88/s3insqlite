use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database_path: String,
    pub buckets: Vec<String>, // List of allowed buckets
    pub port: u16,
    pub bind_address: String,
    pub log_path: String,
    pub log_level: String,                // Add log_level field
    max_workers: Option<usize>,           // Optional for backward compatibility
    max_object_size: Option<usize>,       // Maximum object size in bytes, default to 1 MB
    db_pool_max_size: Option<u32>,        // Maximum number of connections in pool
    db_pool_min_idle: Option<u32>,        // Minimum idle connections to maintain
    db_pool_timeout_seconds: Option<u64>, // Connection acquisition timeout
}

impl AppConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path.as_ref().to_str().unwrap()))
            .build()?;
        settings.try_deserialize()
    }

    pub fn get_max_workers(&self) -> usize {
        self.max_workers.unwrap_or_else(num_cpus::get) // Default to number of CPU cores
    }

    pub fn get_max_object_size(&self) -> usize {
        self.max_object_size.unwrap_or(1024 * 1024 * 1024) // Default to 1 GB
    }

    pub fn get_db_pool_max_size(&self) -> u32 {
        self.db_pool_max_size.unwrap_or(8) // Default to 8 connections
    }

    pub fn get_db_pool_min_idle(&self) -> u32 {
        self.db_pool_min_idle.unwrap_or(2) // Default to 2 idle connections
    }

    pub fn get_db_pool_timeout_seconds(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.db_pool_timeout_seconds.unwrap_or(30))
    }
}
