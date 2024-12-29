package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/schollz/progressbar/v3"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	useSSL          bool
	bucket          string
	sourceFolder    string
	workers         int
	batchSize       int
	maxRetries      int
	dbFile          string
	projectName     string
	debug           bool
}

type FileOp struct {
	sourceKey string
	targetKey string
	yearMonth string
	metadata  map[string]string
}

type Stats struct {
	totalObjects     atomic.Int64
	processedObjects atomic.Int64
	skippedCount     atomic.Int64
	errCount         atomic.Int64
	currentBatch     string
	startTime        time.Time
	totalInDB        int64
	errorLogFile     *os.File
}

type FileMetadata struct {
	ExistingID string `json:"existing_id"`
	IDProfile  string `json:"id_profile"`
	NamaFile   string `json:"nama_file_asli"`
	NamaModul  string `json:"nama_modul"`
}

func displayProgress(stats *Stats, bar *progressbar.ProgressBar) {
	processed := stats.processedObjects.Load()
	errors := stats.errCount.Load()
	skipped := stats.skippedCount.Load()
	total := stats.totalInDB
	elapsed := time.Since(stats.startTime).Seconds()
	speed := float64(processed) / elapsed

	remaining := total - processed - errors - skipped
	eta := time.Duration(float64(remaining) / speed) * time.Second

	percentage := float64(processed+errors+skipped) / float64(total) * 100

	description := fmt.Sprintf("\rBatch: %s | Progress: %.1f%% | Processed: %d | Failed: %d | Skipped: %d | Speed: %.0f files/s | ETA: %v",
		stats.currentBatch,
		percentage,
		processed,
		errors,
		skipped,
		speed,
		eta.Round(time.Second))

	bar.Describe(description)
	bar.Set64(processed)
}

func createErrorLogFile(projectName string) (*os.File, error) {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %v", err)
	}

	// Create or append to error log file
	timestamp := time.Now().Format("2006-01-02")
	filename := fmt.Sprintf("logs/%s_%s_errors.log", projectName, timestamp)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create error log file: %v", err)
	}

	// Write header if file is new
	fileInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if fileInfo.Size() == 0 {
		headerText := fmt.Sprintf("Error log for project %s - Created at %s\n\n", 
			projectName, time.Now().Format("2006-01-02 15:04:05"))
		if _, err := f.WriteString(headerText); err != nil {
			f.Close()
			return nil, err
		}
	}

	return f, nil
}

func logError(f *os.File, sourceKey, errMsg string) {
	if f == nil {
		return
	}
	
	logEntry := fmt.Sprintf("[%s] File: %s - Error: %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		sourceKey,
		errMsg)
	
	if _, err := f.WriteString(logEntry); err != nil {
		log.Printf("Failed to write to error log: %v", err)
	}
}

func debugLog(config Config, format string, args ...interface{}) {
	if config.debug {
		log.Printf(format, args...)
	}
}

func main() {
	config := parseFlags()

	// Initialize MinIO client with custom transport
	customTransport := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:      90 * time.Second,
		MaxIdleConns:         100,
		MaxIdleConnsPerHost:  100,
	}

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure:    config.useSSL,
		Transport: customTransport,
	})
	if err != nil {
		log.Fatalf("Error initializing MinIO client: %v", err)
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", config.dbFile)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Check database schema and counts
	log.Printf("Checking database %s...", config.dbFile)
	checkDatabase(db, config)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create error log file
	errorLogFile, err := createErrorLogFile(config.projectName)
	if err != nil {
		log.Printf("Warning: Failed to create error log file: %v", err)
	} else {
		defer errorLogFile.Close()
	}

	// Channel for work items
	workChan := make(chan FileOp, config.batchSize)

	// Initialize stats
	stats := &Stats{
		startTime:    time.Now(),
		errorLogFile: errorLogFile,
	}

	// Get total count from database
	var totalCount int64
	err = db.QueryRow("SELECT COUNT(*) FROM files WHERE project_name = ? AND status = 'uploaded'", config.projectName).Scan(&totalCount)
	if err != nil {
		log.Fatalf("Failed to get total count: %v", err)
	}
	stats.totalInDB = totalCount

	// Create WaitGroup for workers
	var wg sync.WaitGroup

	// Start progress tracking
	fmt.Printf("Processing files from database %s for project %s...\n", config.dbFile, config.projectName)

	// Start workers
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go worker(ctx, minioClient, config.bucket, workChan, &wg, stats)
	}

	// Create progress bar
	bar := progressbar.NewOptions64(
		totalCount,
		progressbar.OptionSetDescription("Starting..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Println("\nOperation completed!")
		}),
	)

	// Start progress update goroutine
	go func() {
		for {
			displayProgress(stats, bar)
			time.Sleep(time.Second)
		}
	}()

	// Start processing files from database
	go func() {
		defer close(workChan)
		processFilesFromDB(ctx, db, config, workChan, stats)
	}()

	// Wait for all workers to complete
	wg.Wait()

	// Final progress update
	bar.Finish()
	fmt.Printf("\nSummary:\n")
	fmt.Printf("Processed: %d files\n", stats.processedObjects.Load())
	fmt.Printf("Errors: %d\n", stats.errCount.Load())
	fmt.Printf("Skipped: %d\n", stats.skippedCount.Load())
}

func processFilesFromDB(ctx context.Context, db *sql.DB, config Config, workChan chan<- FileOp, stats *Stats) {
	offset := 0
	for {
		query := `
			SELECT id_file, filepath, f_metadata
			FROM files
			WHERE project_name = ? AND status = 'uploaded'
			ORDER BY id
			LIMIT ? OFFSET ?`

		debugLog(config, "Running query with project_name=%s, limit=%d, offset=%d", config.projectName, config.batchSize, offset)

		rows, err := db.QueryContext(ctx, query, config.projectName, config.batchSize, offset)
		if err != nil {
			log.Printf("Error querying database: %v", err)
			return
		}

		empty := true
		batchCount := 0
		for rows.Next() {
			empty = false
			batchCount++

			var idFile, filepath, metadataStr string
			if err := rows.Scan(&idFile, &filepath, &metadataStr); err != nil {
				log.Printf("Error scanning row: %v", err)
				stats.errCount.Add(1)
				continue
			}

			debugLog(config, "Found record: id_file=%s, filepath=%s, metadata=%s", idFile, filepath, metadataStr)

			// Parse metadata
			var metadata FileMetadata
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
				log.Printf("Error parsing metadata for %s: %v", idFile, err)
				stats.errCount.Add(1)
				continue
			}

			// Extract year-month from idFile (format: U-YYYYMM...)
			if len(idFile) < 9 {
				log.Printf("Invalid id_file format: %s", idFile)
				stats.skippedCount.Add(1)
				continue
			}

			yearMonth := idFile[2:8] // Extract YYYYMM
			targetKey := path.Join("download", yearMonth, path.Base(idFile))
			sourceKey := path.Join(config.sourceFolder, idFile)

			stats.totalObjects.Add(1)
			stats.currentBatch = fmt.Sprintf("Batch %d-%d", offset, offset+batchCount)

			select {
			case <-ctx.Done():
				rows.Close()
				return
			case workChan <- FileOp{
				sourceKey: sourceKey,
				targetKey: targetKey,
				yearMonth: yearMonth,
				metadata: map[string]string{
					"X-Amz-Meta-Original-Path": filepath,
					"X-Amz-Meta-Module":        metadata.NamaModul,
					"X-Amz-Meta-Original-Name": metadata.NamaFile,
				},
			}:
			}
		}
		rows.Close()

		debugLog(config, "Batch completed: found %d records", batchCount)

		if empty {
			debugLog(config, "No more records found, exiting")
			break
		}

		offset += config.batchSize
	}
}

func worker(ctx context.Context, client *minio.Client, bucket string, workChan <-chan FileOp,
	wg *sync.WaitGroup, stats *Stats) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case work, ok := <-workChan:
			if !ok {
				return
			}

			// Check if source object exists first
			_, err := client.StatObject(ctx, bucket, work.sourceKey, minio.StatObjectOptions{})
			if err != nil {
				if strings.Contains(err.Error(), "The specified key does not exist") {
					// Log to error file and skip retries for missing files
					logError(stats.errorLogFile, work.sourceKey, "File does not exist in MinIO")
					stats.errCount.Add(1)
					continue
				}
			}

			var lastErr error
			success := false

			// Only retry if it's not a "key does not exist" error
			for attempts := 0; attempts < 3; attempts++ {
				if attempts > 0 {
					log.Printf("Retry %d for file %s", attempts, work.sourceKey)
					time.Sleep(time.Second * time.Duration(attempts))
				}

				// Copy object with metadata
				srcOpts := minio.CopySrcOptions{
					Bucket: bucket,
					Object: work.sourceKey,
				}

				dstOpts := minio.CopyDestOptions{
					Bucket:          bucket,
					Object:          work.targetKey,
					UserMetadata:    work.metadata,
					ReplaceMetadata: true,
				}

				_, err := client.CopyObject(ctx, dstOpts, srcOpts)
				if err != nil {
					lastErr = fmt.Errorf("error copying: %v", err)
					continue
				}

				// Remove old object
				err = client.RemoveObject(ctx, bucket, work.sourceKey, minio.RemoveObjectOptions{})
				if err != nil {
					lastErr = fmt.Errorf("error removing: %v", err)
					continue
				}

				success = true
				break
			}

			if !success {
				log.Printf("Failed to process %s after retries: %v", work.sourceKey, lastErr)
				logError(stats.errorLogFile, work.sourceKey, lastErr.Error())
				stats.errCount.Add(1)
				continue
			}

			stats.processedObjects.Add(1)
		}
	}
}

func checkDatabase(db *sql.DB, config Config) {
	// Check table schema
	rows, err := db.Query("SELECT sql FROM sqlite_master WHERE type='table' AND name='files'")
	if err != nil {
		log.Printf("Error checking table schema: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var tableSQL string
		if err := rows.Scan(&tableSQL); err != nil {
			log.Printf("Error reading table schema: %v", err)
		} else {
			debugLog(config, "Table schema: %s", tableSQL)
		}
	}

	// Get total count and project names
	rows, err = db.Query("SELECT project_name, status, COUNT(*) FROM files GROUP BY project_name, status")
	if err != nil {
		log.Printf("Error getting file counts: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var projectName, status string
		var count int
		if err := rows.Scan(&projectName, &status, &count); err != nil {
			log.Printf("Error reading count: %v", err)
			continue
		}
		debugLog(config, "Project: %s, Status: %s, Count: %d", projectName, status, count)
	}
}

func parseFlags() Config {
	endpoint := flag.String("endpoint", "", "MinIO endpoint (required)")
	accessKey := flag.String("access-key", "", "Access key (required)")
	secretKey := flag.String("secret-key", "", "Secret key (required)")
	useSSL := flag.Bool("use-ssl", true, "Use SSL for connection")
	bucket := flag.String("bucket", "", "Target bucket (required)")
	sourceFolder := flag.String("source-folder", "download", "Source folder")
	workers := flag.Int("workers", 10, "Number of worker goroutines")
	batchSize := flag.Int("batch-size", 1000, "Number of objects to process per batch")
	maxRetries := flag.Int("max-retries", 3, "Maximum number of retries for operations")
	dbFile := flag.String("db-file", "", "SQLite database file path (required)")
	projectName := flag.String("project-name", "", "Project name to process from database (required)")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	// Validate required flags
	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" || *dbFile == "" || *projectName == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Validate batch size
	if *batchSize < 100 || *batchSize > 10000 {
		log.Fatal("Batch size must be between 100 and 10000")
	}

	// Clean source folder path
	cleanSourceFolder := strings.Trim(*sourceFolder, "/")

	return Config{
		endpoint:        *endpoint,
		accessKeyID:     *accessKey,
		secretAccessKey: *secretKey,
		useSSL:         *useSSL,
		bucket:         *bucket,
		sourceFolder:   cleanSourceFolder,
		workers:        *workers,
		batchSize:      *batchSize,
		maxRetries:     *maxRetries,
		dbFile:         *dbFile,
		projectName:    *projectName,
		debug:          *debug,
	}
}
