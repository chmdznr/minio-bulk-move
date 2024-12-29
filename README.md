# MinIO Bulk Move

A Go application for efficiently moving files in MinIO storage based on their year-month pattern. This tool uses a SQLite database to track files and their metadata, making it much more efficient than listing objects directly from MinIO.

## Features

- Database-driven file processing for improved performance
- Batch processing with configurable batch size
- Concurrent processing with multiple workers
- Progress bar showing real-time processing status
- Detailed processing statistics
- Support for MinIO S3-compatible storage
- Configurable retry mechanism for resilient operations
- Real-time progress tracking with batch information
- Metadata preservation and enhancement
- Optimized HTTP transport settings

## Prerequisites

- Go 1.23 or higher
- Access to a MinIO server
- MinIO credentials (access key and secret key)
- SQLite database with file information

## Installation

```bash
# Clone the repository
git clone [your-repo-url]
cd minio-bulk-move

# Install dependencies
go mod download

# Build static binary
go build -o minio-bulk-move
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

```bash
# For Windows
.\minio-bulk-move.exe [flags]

# For Unix-like systems
./minio-bulk-move [flags]
```

### Available Flags

- `-endpoint`: MinIO server endpoint (required)
- `-access-key`: MinIO access key (required)
- `-secret-key`: MinIO secret key (required)
- `-use-ssl`: Use SSL for MinIO connection
- `-bucket`: Source bucket name (required)
- `-source-folder`: Source folder path in the bucket
- `-workers`: Number of concurrent workers (default: 10)
- `-batch-size`: Number of files to process per batch (default: 1000)
- `-max-retries`: Maximum number of retries for operations (default: 3)
- `-db-file`: Path to SQLite database file (required)
- `-project-name`: Project name to process from database (required)

### Example

```powershell
# For Windows PowerShell
.\minio-bulk-move.exe `
  -endpoint localhost:9000 `
  -access-key YOUR_ACCESS_KEY `
  -secret-key YOUR_SECRET_KEY `
  -bucket your-bucket-name `
  -source-folder download `
  -workers 10 `
  -batch-size 1000 `
  -max-retries 3 `
  -use-ssl=false `
  -db-file your-database.db `
  -project-name your-project-name

# For Unix-like systems (bash)
./minio-bulk-move \
  -endpoint localhost:9000 \
  -access-key YOUR_ACCESS_KEY \
  -secret-key YOUR_SECRET_KEY \
  -bucket your-bucket-name \
  -source-folder download \
  -workers 10 \
  -batch-size 1000 \
  -max-retries 3 \
  -use-ssl=false \
  -db-file ./your-database.db \
  -project-name your-project-name
```

The command will:
1. Connect to local MinIO server on port 9000
2. Process files from the specified project in the database
3. Move files from their current location to year-month based folders
4. Preserve and enhance metadata (original path, module name, etc.)
5. Show progress with current batch information

## Database Usage

The tool uses the SQLite database to:
1. Get list of files to process (avoiding slow MinIO ListObjects)
2. Track file metadata and original paths
3. Process files in efficient batches

The tool expects files in the database to have:
- `id_file`: The object name in MinIO
- `filepath`: Original file path
- `f_metadata`: JSON metadata with fields:
  ```json
  {
    "existing_id": "P-XXXXXXXXXX",
    "id_profile": "XXXXXXXXX",
    "nama_file_asli": "Original filename.pdf",
    "nama_modul": "Module name"
  }
  ```
- `status`: Should be 'uploaded' for files to process

### Example Database Record
```sql
SELECT id_file, filepath, f_metadata FROM files LIMIT 1;

-- Result:
-- id_file: "U-202112150934130542920"
-- filepath: "documents/P-202103311006210636094/folder1/DOCUMENT.pdf"
-- f_metadata: {
--   "existing_id": "P-202103311006210636094",
--   "id_profile": "1220183059",
--   "nama_file_asli": "DOCUMENT.pdf",
--   "nama_modul": "documents"
-- }
```

## Performance Tips

When dealing with massive file sets:

1. **Batch Size**: Adjust `-batch-size` based on your system's memory capacity. Lower values (500-1000) are safer but slower, higher values may be faster but use more memory.

2. **Workers**: The `-workers` flag controls concurrent operations. Start with 10 and adjust based on:
   - Available system resources
   - Network capacity
   - MinIO server capacity

3. **Retries**: Use `-max-retries` to handle temporary failures. Default is 3, increase in unstable networks.

4. **Database Indexes**: The application relies on database indexes for efficient querying. Make sure the recommended indexes are created.

5. **Network**: Ensure stable network connection as the tool performs multiple operations:
   - Reading from SQLite database
   - Copying objects in MinIO
   - Removing original objects
   - Updating metadata

## Dependencies

- github.com/minio/minio-go/v7 - MinIO Go client
- github.com/schollz/progressbar/v3 - Progress bar visualization
- github.com/mattn/go-sqlite3 - SQLite driver for Go

## License

BSD-3-Clause
