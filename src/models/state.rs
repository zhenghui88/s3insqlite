use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::collections::HashSet;

/// Application state shared across all request handlers
pub struct AppState {
    pub db_pool: Pool<SqliteConnectionManager>,
    pub buckets: HashSet<String>, // The expected buckets
}
