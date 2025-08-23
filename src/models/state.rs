use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::collections::HashSet;
use std::sync::Arc;

/// Application state shared across all request handlers
#[derive(Clone)]
pub struct AppState {
    pub db_pool: Arc<Pool<SqliteConnectionManager>>,
    pub buckets: Arc<HashSet<String>>, // The expected buckets
}

impl AppState {
    pub fn new(db_pool: Pool<SqliteConnectionManager>, buckets: HashSet<String>) -> Self {
        Self {
            db_pool: Arc::new(db_pool),
            buckets: Arc::new(buckets),
        }
    }
}
