use chrono::Utc;
use log;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Initialize the logger with the specified log level and output file
pub fn initialize_logger<P: AsRef<Path>>(
    log_path: P,
    log_level_str: &str,
) -> Result<(), log::SetLoggerError> {
    // Parse log level from config string
    let log_level = match log_level_str.parse::<log::LevelFilter>() {
        Ok(level) => level,
        Err(_) => log::LevelFilter::Debug, // Default to Debug if invalid
    };

    // Setup logging to file
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("Failed to open log file");

    let log_file = Mutex::new(log_file);

    let logger = env_logger::Builder::new()
        .format(move |buf, record| {
            // Write to log file
            if let Ok(mut file) = log_file.lock() {
                let timestamp = Utc::now().to_rfc3339();
                let log_line = format!("{} [{}] - {}\n", timestamp, record.level(), record.args());
                let _ = file.write_all(log_line.as_bytes());
            }

            // Also write to stderr (console)
            writeln!(
                buf,
                "{} [{}] - {}",
                Utc::now().to_rfc3339(),
                record.level(),
                record.args()
            )
        })
        .filter_level(log_level)
        .build();

    // Set the global logger
    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(log_level);

    Ok(())
}
