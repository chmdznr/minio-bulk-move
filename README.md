# MinIO Bulk Move

A Go application for efficiently moving files in MinIO storage based on their year-month pattern. This tool helps organize files by processing them in batches with concurrent workers.

## Features

- Batch processing of files with configurable batch size
- Concurrent processing with multiple workers
- Progress bar showing real-time processing status
- Year-based file filtering and organization
- Detailed processing statistics
- Support for MinIO S3-compatible storage

## Prerequisites

- Go 1.23 or higher
- Access to a MinIO server
- MinIO credentials (access key and secret key)

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
- `-base-year`: Base year for file filtering
- `-workers`: Number of concurrent workers (default: 5)
- `-batch-size`: Batch size for processing (default: 1000)

### Example

```bash
# For Windows
.\minio-bulk-move.exe \
  -endpoint play.min.io \
  -access-key YOUR_ACCESS_KEY \
  -secret-key YOUR_SECRET_KEY \
  -bucket my-bucket \
  -source-folder path/to/files \
  -base-year 2023 \
  -workers 10 \
  -batch-size 2000

# For Unix-like systems
./minio-bulk-move \
  -endpoint play.min.io \
  -access-key YOUR_ACCESS_KEY \
  -secret-key YOUR_SECRET_KEY \
  -bucket my-bucket \
  -source-folder path/to/files \
  -base-year 2023 \
  -workers 10 \
  -batch-size 2000
```

## Dependencies

- github.com/minio/minio-go/v7 - MinIO Go client
- github.com/schollz/progressbar/v3 - Progress bar visualization

## License

BSD-3-Clause
