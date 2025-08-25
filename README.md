# s3insqlite

`s3insqlite` is a Rust application that provides S3-compatible object storage backed by SQLite. It is designed for lightweight and local use cases where a full S3 service is unnecessary but S3-like semantics are desired.

## Features

- **S3-like API**: Upload, download, delete, and list objects using familiar S3-style endpoints.
- **SQLite Backend**: All objects and metadata are stored in a local SQLite database.
- **Configurable Buckets**: Define and manage multiple buckets via configuration.
- **Logging and Concurrency**: Configurable logging and worker pool for concurrent requests.
- **Bucket Validation and Sanitization**: Ensures bucket names are valid and safe.

## Configuration

The application is configured using the `AppConfig` struct, which can be loaded from a file (e.g. `config.toml`. Key configuration options:

- `database_path`: Path to the SQLite database file.
- `buckets`: List of bucket names to manage.
- `port`: Port to bind the HTTP server.
- `bind_address`: Network address to bind.
- `log_path`: Path to the log file.
- `log_level`: Logging verbosity.
- `max_workers`: Maximum number of worker threads.

## Main Components

### AppState

Holds the global application state:

- `db_pool`: Connection pool for SQLite.
- `buckets`: List of configured buckets.

### Core Functions

- **Bucket Management**
  - `list_buckets`: Lists all configured buckets.
  - `sanitize_bucket_name`: Ensures bucket names are safe for use.
  - `validate_bucket`: Checks if a bucket exists and is valid.
  - `get_bucket_versioning`: Retrieves versioning status for a bucket.
  - `list_objects`: Lists objects in a bucket (compatible with S3 ListObjects V1 API).
    - Supports parameters: `prefix`, `delimiter`
  - `list_objects_v2`: Lists objects in a bucket (compatible with S3 ListObjectsV2 API).
    - Supports parameters: `prefix`, `delimiter`

- **Object Operations**
  - `upload_object`: Handles uploading objects to a bucket.
  - `download_object`: Handles downloading objects from a bucket.
  - `delete_object`: Handles deleting objects from a bucket.
  - `head_object`: Retrieves metadata for an object.

### Entry Point

- `main`: Loads configuration, initializes logging and database, and starts the HTTP server.

## Usage

1. **Configure** your buckets and database in a config file (see `AppConfig`).
2. **Run the application**:
   ```bash
   cargo run --release
   ```
3. **Interact** with the service using S3-compatible tools or HTTP requests.

## Example Endpoints

- `GET /` — List all buckets
- `GET /bucket?versioning` — Get bucket versioning status
- `GET /bucket` — List objects in a bucket (ListObjects V1)
- `GET /bucket?list-type=2` — List objects in a bucket (ListObjectsV2)
- `PUT /bucket/object` — Upload an object
- `GET /bucket/object` — Download an object
- `DELETE /bucket/object` — Delete an object
- `HEAD /bucket/object` — Get object metadata

## License

Apache-2.0

---

This project is designed for lightweight local storage use cases and now supports Zarr for reading and writing operations. Please refer to the `tests` directory for usage examples.
