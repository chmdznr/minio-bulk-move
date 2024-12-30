# MinIO Bulk Move

A Go application for efficiently moving files in MinIO storage based on their year-month pattern. This tool uses a SQLite database to track files and their metadata, making it much more efficient than listing objects directly from MinIO.

## Features

- Database-driven file processing for improved performance
- Split command structure for move and cleanup operations
- Batch processing with configurable batch size
- Concurrent processing with multiple workers
- Progress bar showing real-time processing status
- Detailed processing statistics
- Support for MinIO S3-compatible storage
- Configurable retry mechanism for resilient operations
- Real-time progress tracking with batch information
- Metadata preservation and enhancement
- Optimized HTTP transport settings
- Versioned object support with separate cleanup process
- Generates cleanup script with real-time progress tracking

## Prerequisites

- Go 1.23 or higher
- Access to a MinIO server
- MinIO credentials (access key and secret key)
- SQLite database with file information
- MinIO Client (mc) configured for cleanup operations

## Installation

```bash
# Clone the repository
git clone [your-repo-url]
cd minio-bulk-move

# Install dependencies
go mod download

# Build static binary
go build -o minio-bulk-move

# Configure MinIO Client (mc) for cleanup operations
mc alias set minio http://your-minio-server ACCESSKEY SECRETKEY
```

## Database Schema

The application expects a SQLite database with the following schema:

```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL,
    id_file TEXT,
    id_permohonan TEXT,
    id_from_csv TEXT,
    filepath TEXT NOT NULL,
    file_size INTEGER,
    file_type TEXT,
    bucketpath TEXT,
    f_metadata TEXT,
    userid TEXT DEFAULT 'migrator',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    str_key TEXT,
    str_subkey TEXT,
    timestamp TEXT,
    status TEXT DEFAULT 'pending',
    UNIQUE(project_name, filepath)
);

CREATE INDEX idx_files_project_name ON files(project_name);
CREATE INDEX idx_files_filepath ON files(filepath);
CREATE INDEX idx_files_status ON files(status);
```

## Usage

The application supports two main commands: `move` and `cleanup`.

### Move Command

Moves files to their new locations based on year-month pattern:

```bash
./minio-bulk-move move \
    -endpoint localhost:9000 \
    -access-key YOUR_ACCESS_KEY \
    -secret-key YOUR_SECRET_KEY \
    -bucket your-bucket \
    -source-folder "download" \
    -workers 10 \
    -batch-size 1000 \
    -max-retries 3 \
    -db-file path/to/db.sqlite \
    -project-name your-project \
    -use-ssl=false \
    -debug false

# Parameters:
#   -endpoint        : MinIO server endpoint
#   -access-key      : MinIO access key
#   -secret-key      : MinIO secret key
#   -bucket          : Source bucket name
#   -source-folder   : Source folder path in bucket
#   -workers         : Number of concurrent workers (default: 10)
#   -batch-size      : Number of files to process per batch (default: 1000)
#   -max-retries     : Maximum number of retries for operations (default: 3)
#   -db-file         : Path to SQLite database file
#   -project-name    : Project name to process from database
#   -use-ssl         : Use SSL for MinIO connection (default: false)
#   -debug           : Enable debug logging (default: false)
```

### Cleanup Command

Generates and executes a script to clean up versioned objects:

```bash
# Step 1: Generate cleanup script
./minio-bulk-move cleanup \
    -endpoint localhost:9000 \
    -access-key YOUR_ACCESS_KEY \
    -secret-key YOUR_SECRET_KEY \
    -bucket your-bucket \
    -source-folder "download" \
    -batch-size 1000 \
    -db-file path/to/db.sqlite \
    -project-name your-project \
    -alias minio \
    -use-ssl=false \
    -debug false

# Parameters:
#   -endpoint        : MinIO server endpoint
#   -access-key      : MinIO access key
#   -secret-key      : MinIO secret key
#   -bucket          : Source bucket name
#   -source-folder   : Source folder path in bucket
#   -batch-size      : Number of files to process per batch (default: 1000)
#   -db-file         : Path to SQLite database file
#   -project-name    : Project name to process from database
#   -alias           : MinIO alias for mc command
#   -use-ssl         : Use SSL for MinIO connection (default: false)
#   -debug           : Enable debug logging (default: false)

# Step 2: Configure MinIO Client (mc)
mc alias set minio http://localhost:9000 YOUR_ACCESS_KEY YOUR_SECRET_KEY

# Step 3: Run the cleanup script
bash cleanup_YYYYMMDD_HHMMSS.sh
```

The cleanup process works in two phases:
1. **Move Operation**:
   - Reads files from SQLite database
   - Constructs new path based on year-month pattern
   - Copies file with preserved metadata to new location
   - Marks the file for cleanup in the database

2. **Cleanup Operation**:
   - Generates a shell script to remove versioned objects
   - Shows real-time progress during cleanup
   - Logs errors to cleanup_errors.log
   - Saves progress to cleanup_progress.log
   - Can be interrupted and resumed safely

## Progress Tracking

Both move and cleanup operations provide detailed progress information:

1. **Move Command**:
   - Progress bar showing percentage complete
   - Current batch information
   - Estimated time remaining
   - Error count and skipped files
   - Processing rate (files/second)

2. **Cleanup Script**:
   - Real-time progress percentage
   - Processing rate (files/second)
   - Estimated time remaining (ETA)
   - Error logging with file details
   - Progress log file for monitoring

## Error Handling

- Failed operations are logged to error.log
- Retry mechanism for transient failures
- Detailed error messages with file paths
- Safe to re-run operations (idempotent)
- Cleanup script can be interrupted and resumed

## Best Practices

1. **Database Performance**:
   - Create appropriate indexes
   - Regular VACUUM to optimize space
   - Monitor database size growth

2. **Resource Usage**:
   - Adjust workers based on CPU cores
   - Monitor memory usage with large batches
   - Consider network bandwidth limitations

3. **Operation Safety**:
   - Always backup database before operations
   - Review cleanup script before execution
   - Monitor error logs during processing
   - Use debug mode for troubleshooting

## License

This project is licensed under the MIT License - see the LICENSE file for details.
