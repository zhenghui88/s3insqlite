use log::error;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use std::time::Duration;

/// Create and configure an optimized SQLite connection pool
pub fn create_connection_pool(
    db_path: &str,
    max_size: u32,
    min_idle: u32,
    timeout_seconds: u64,
) -> Result<Pool<SqliteConnectionManager>, r2d2::Error> {
    // Create a manager that enables WAL mode and other optimizations
    let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA cache_size = 1000;
             PRAGMA foreign_keys = OFF;
             PRAGMA busy_timeout = 5000;",
        )
    });

    // Configure the connection pool
    r2d2::Pool::builder()
        .max_size(max_size)
        .min_idle(Some(min_idle))
        .max_lifetime(None) // Connections last until closed
        .idle_timeout(Some(Duration::from_secs(300))) // 5 minutes idle timeout
        .connection_timeout(Duration::from_secs(timeout_seconds))
        .build(manager)
}

/// Create indexes for a bucket table to improve query performance
pub fn create_bucket_indexes(conn: &Connection, table_name: &str) -> rusqlite::Result<()> {
    // Create an index on the key column for faster lookups
    let index_sql = format!(
        "CREATE INDEX IF NOT EXISTS idx_{}_key ON {} (key)",
        table_name, table_name
    );
    conn.execute(&index_sql, [])?;

    Ok(())
}

/// Optimize the database by running VACUUM and ANALYZE
pub fn optimize_database(pool: &Pool<SqliteConnectionManager>) -> rusqlite::Result<()> {
    let conn = pool
        .get()
        .map_err(|_e| rusqlite::Error::QueryReturnedNoRows)?;

    // Run VACUUM to reclaim unused space
    conn.execute("VACUUM", [])?;

    // Run ANALYZE to update statistics for the query planner
    conn.execute("ANALYZE", [])?;

    Ok(())
}

/// Schedule periodic database optimization in a background task
pub fn schedule_optimization(pool: Pool<SqliteConnectionManager>) {
    // Clone the pool for the background task
    let pool_clone = pool.clone();

    // Spawn a background task to periodically optimize the database
    tokio::spawn(async move {
        let interval = Duration::from_secs(3600 * 24); // Once per day
        let mut interval = tokio::time::interval(interval);

        loop {
            interval.tick().await;
            if let Err(e) = optimize_database(&pool_clone) {
                error!("Database optimization failed: {}", e);
            } else {
                log::info!("Scheduled database optimization completed successfully");
            }
        }
    });
}
