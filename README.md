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

```powershell
.\minio-bulk-move.exe move `
    -endpoint localhost:9000 `
    -access-key YOUR_ACCESS_KEY `
    -secret-key YOUR_SECRET_KEY `
    -use-ssl false `
    -bucket your-bucket `
    -source-folder "download" `
    -workers 10 `
    -batch-size 1000 `
    -max-retries 3 `
    -db-file path/to/db.sqlite `
    -project-name your-project `
    -debug false
```

### Cleanup Command

Instead of directly removing versioned objects (which can be very slow with millions of files), the cleanup command generates a shell script with progress tracking:

```powershell
.\minio-bulk-move.exe cleanup `
    -endpoint localhost:9000 `
    -access-key YOUR_ACCESS_KEY `
    -secret-key YOUR_SECRET_KEY `
    -use-ssl false `
    -bucket your-bucket `
    -source-folder "download" `
    -db-file path/to/db.sqlite `
    -batch-size 1000 `
    -debug false
```

This will generate a script (`cleanup_YYYYMMDD_HHMMSS.sh`) that:
- Shows real-time progress in terminal
- Saves progress to `cleanup_progress.log`
- Logs errors to `cleanup_errors.log`
- Can be interrupted and resumed
- Tracks processing rate and ETA

Example cleanup script output:
```
Progress: 45.2% | Processed: 452/1000 | Failed: 2 | Rate: 0.25 files/sec | ETA: 06:30:45
```

### Available Flags

#### Move Command Flags
- `-endpoint`: MinIO server endpoint (required)
- `-access-key`: MinIO access key (required)
- `-secret-key`: MinIO secret key (required)
- `-use-ssl`: Use SSL for MinIO connection
- `-bucket`: Source bucket name (required)
- `-source-folder`: Source folder path in bucket (required)
- `-workers`: Number of concurrent workers (default: 10)
- `-batch-size`: Number of files to process per batch (default: 1000)
- `-max-retries`: Maximum number of retries for operations (default: 3)
- `-db-file`: Path to SQLite database file (required)
- `-project-name`: Project name to process from database (required)
- `-debug`: Enable verbose debug logging (default: false)

#### Cleanup Command Flags
- `-endpoint`: MinIO server endpoint (required)
- `-access-key`: MinIO access key (required)
- `-secret-key`: MinIO secret key (required)
- `-use-ssl`: Use SSL for MinIO connection
- `-bucket`: Source bucket name (required)
- `-source-folder`: Source folder path in bucket (required)
- `-db-file`: Path to SQLite database file (required)
- `-batch-size`: Number of files to process per batch (default: 1000)
- `-debug`: Enable verbose debug logging (default: false)

## File Processing Flow

1. **Move Operation**:
   - Reads files from SQLite database
   - Constructs new path based on year-month pattern
   - Copies file with preserved metadata to new location
   - Removes the latest version of the source file
   - Marks file as 'pending_cleanup' in database

2. **Cleanup Operation**:
   - Generates a shell script to handle version cleanup
   - Script uses MinIO Client (mc) for efficient version removal
   - Includes progress tracking and error logging
   - Can be run independently during off-peak hours

## Error Handling

- Failed moves are logged with detailed error messages
- Failed cleanups are logged to `cleanup_errors.log`
- All operations support configurable retries
- Detailed error logs are saved to `logs/[project_name]_[date]_errors.log`

## Performance Considerations

- Use appropriate batch size based on your system resources
- Adjust worker count based on available CPU cores
- Run cleanup script during off-peak hours
- Monitor MinIO server load during bulk operations
- Consider splitting cleanup script for parallel execution
- Version cleanup can take several minutes per file

## Dependencies

- github.com/minio/minio-go/v7 - MinIO Go client
- github.com/schollz/progressbar/v3 - Progress bar visualization
- github.com/mattn/go-sqlite3 - SQLite driver for Go

## License

BSD-3-Clause
