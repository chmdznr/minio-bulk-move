# MinIO Bulk Move

A Go application for efficiently moving files in MinIO storage based on their year-month pattern. This tool helps organize files by processing them in batches with concurrent workers.

## Features

- Batch processing of files with configurable batch size
- Concurrent processing with multiple workers
- Progress bar showing real-time processing status
- Year-based file filtering and organization
- Detailed processing statistics
- Support for MinIO S3-compatible storage
- Prefix-based processing for efficient handling of large file sets
- Configurable retry mechanism for resilient operations
- Real-time progress tracking with current prefix display
- Optimized HTTP transport settings for better performance

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
- `-workers`: Number of concurrent workers (default: 10)
- `-batch-size`: Batch size for processing (default: 1000)
- `-max-retries`: Maximum number of retries for operations (default: 3)

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
  -batch-size 1000 \
  -max-retries 3

# For Unix-like systems
./minio-bulk-move \
  -endpoint play.min.io \
  -access-key YOUR_ACCESS_KEY \
  -secret-key YOUR_SECRET_KEY \
  -bucket my-bucket \
  -source-folder path/to/files \
  -base-year 2023 \
  -workers 10 \
  -batch-size 1000 \
  -max-retries 3
```

## Performance Tips

When dealing with massive file sets (millions of files):

1. **Batch Size**: Adjust `-batch-size` based on your system's memory capacity. Lower values (500-1000) are safer but slower, higher values may be faster but use more memory.

2. **Workers**: The `-workers` flag controls concurrent operations. Start with 10 and adjust based on:
   - Available system resources
   - Network capacity
   - MinIO server capacity

3. **Retries**: Use `-max-retries` to handle temporary failures. Default is 3, increase in unstable networks.

4. **Memory Usage**: The tool processes files by year-month prefixes to manage memory efficiently.

5. **Network**: Ensure stable network connection as the tool performs multiple operations:
   - Listing objects
   - Reading metadata
   - Copying objects
   - Removing original objects

## Dependencies

- github.com/minio/minio-go/v7 - MinIO Go client
- github.com/schollz/progressbar/v3 - Progress bar visualization

## License

BSD-3-Clause
