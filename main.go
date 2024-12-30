package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
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
	alias           string
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

var (
	dbMutex sync.Mutex
	dbPool  *sql.DB
)

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

func initDB(dbFile string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(1)  // SQLite only supports one writer at a time
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	// Set busy timeout to avoid database locked errors
	_, err = db.Exec("PRAGMA busy_timeout = 5000")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %v", err)
	}

	return db, nil
}

func markFileForCleanup(ctx context.Context, filepath string, dbFile string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Set busy timeout to avoid database locked errors
	_, err = db.Exec("PRAGMA busy_timeout = 5000")
	if err != nil {
		return fmt.Errorf("failed to set busy timeout: %v", err)
	}

	query := `UPDATE files SET status = 'pending_cleanup' WHERE id_file = ?`
	_, err = db.ExecContext(ctx, query, path.Base(filepath))
	if err != nil {
		return fmt.Errorf("error marking for cleanup: %v", err)
	}
	return nil
}

func main() {
	moveCmd := flag.NewFlagSet("move", flag.ExitOnError)
	cleanupCmd := flag.NewFlagSet("cleanup", flag.ExitOnError)

	moveConfig := Config{}
	moveCmd.StringVar(&moveConfig.endpoint, "endpoint", "", "MinIO server endpoint")
	moveCmd.StringVar(&moveConfig.accessKeyID, "access-key", "", "MinIO access key")
	moveCmd.StringVar(&moveConfig.secretAccessKey, "secret-key", "", "MinIO secret key")
	moveCmd.BoolVar(&moveConfig.useSSL, "use-ssl", false, "Use SSL for MinIO connection")
	moveCmd.StringVar(&moveConfig.bucket, "bucket", "", "Source bucket name")
	moveCmd.StringVar(&moveConfig.sourceFolder, "source-folder", "", "Source folder path in bucket")
	moveCmd.IntVar(&moveConfig.workers, "workers", 10, "Number of concurrent workers")
	moveCmd.IntVar(&moveConfig.batchSize, "batch-size", 1000, "Number of files to process per batch")
	moveCmd.IntVar(&moveConfig.maxRetries, "max-retries", 3, "Maximum number of retries for operations")
	moveCmd.StringVar(&moveConfig.dbFile, "db-file", "", "Path to SQLite database file")
	moveCmd.StringVar(&moveConfig.projectName, "project-name", "", "Project name to process from database")
	moveCmd.BoolVar(&moveConfig.debug, "debug", false, "Enable debug logging")
	moveConfig.alias = ""

	cleanupConfig := Config{}
	cleanupCmd.StringVar(&cleanupConfig.endpoint, "endpoint", "", "MinIO server endpoint")
	cleanupCmd.StringVar(&cleanupConfig.accessKeyID, "access-key", "", "MinIO access key")
	cleanupCmd.StringVar(&cleanupConfig.secretAccessKey, "secret-key", "", "MinIO secret key")
	cleanupCmd.StringVar(&cleanupConfig.bucket, "bucket", "", "Source bucket name")
	cleanupCmd.StringVar(&cleanupConfig.sourceFolder, "source-folder", "", "Source folder path in bucket")
	cleanupCmd.IntVar(&cleanupConfig.batchSize, "batch-size", 1000, "Number of files to process per batch")
	cleanupCmd.StringVar(&cleanupConfig.dbFile, "db-file", "", "Path to SQLite database file")
	cleanupCmd.StringVar(&cleanupConfig.projectName, "project-name", "", "Project name to process from database")
	cleanupCmd.StringVar(&cleanupConfig.alias, "alias", "", "MinIO alias for mc command")
	cleanupCmd.BoolVar(&cleanupConfig.useSSL, "use-ssl", false, "Use SSL for MinIO connection")
	cleanupCmd.BoolVar(&cleanupConfig.debug, "debug", false, "Enable debug logging")

	if len(os.Args) < 2 {
		fmt.Println("Expected 'move' or 'cleanup' subcommand")
		fmt.Println("\nUsage:")
		fmt.Println("  move     - Move files and mark for version cleanup")
		fmt.Println("  cleanup  - Clean up versions of previously moved files")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "move":
		moveCmd.Parse(os.Args[2:])
		runMove(moveConfig)
	case "cleanup":
		cleanupCmd.Parse(os.Args[2:])
		runCleanup(cleanupConfig)
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		os.Exit(1)
	}
}

func runMove(config Config) {
	if config.endpoint == "" || config.accessKeyID == "" || config.secretAccessKey == "" ||
		config.bucket == "" || config.dbFile == "" || config.projectName == "" {
		log.Fatal("All required flags must be provided")
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure: config.useSSL,
	})
	if err != nil {
		log.Fatalf("Error creating MinIO client: %v", err)
	}

	stats := &Stats{
		startTime: time.Now(),
	}

	errorLogFile, err := createErrorLogFile(config.projectName)
	if err != nil {
		log.Fatalf("Error creating error log file: %v", err)
	}
	defer errorLogFile.Close()
	stats.errorLogFile = errorLogFile

	dbPool, err = initDB(config.dbFile)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer dbPool.Close()

	// Get total count first
	var totalCount int64
	err = dbPool.QueryRow("SELECT COUNT(*) FROM files WHERE project_name = ? AND status = 'uploaded'", config.projectName).Scan(&totalCount)
	if err != nil {
		log.Fatalf("Error getting total count: %v", err)
	}
	stats.totalInDB = totalCount

	workChan := make(chan FileOp, config.batchSize)

	var wg sync.WaitGroup
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go worker(ctx, minioClient, config.bucket, workChan, &wg, stats, config)
	}

	bar := showProgress(stats, totalCount)
	defer bar.Close()

	checkDatabase(dbPool, config)

	processFilesFromDB(ctx, dbPool, config, workChan, stats)

	close(workChan)
	wg.Wait()

	fmt.Printf("\nOperation completed in %v\n", time.Since(stats.startTime))
	fmt.Printf("Total files: %d\n", totalCount)
	fmt.Printf("Processed: %d files\n", stats.processedObjects.Load())
	fmt.Printf("Errors: %d\n", stats.errCount.Load())
	fmt.Printf("Skipped: %d\n", stats.skippedCount.Load())
}

func runCleanup(config Config) {
	// Validate cleanup command flags
	if config.endpoint == "" || config.accessKeyID == "" || config.secretAccessKey == "" ||
		config.bucket == "" || config.dbFile == "" || config.sourceFolder == "" || config.projectName == "" ||
		config.alias == "" {
		log.Fatal("All required flags must be provided")
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure: config.useSSL,
	})
	if err != nil {
		log.Fatalf("Error creating MinIO client: %v", err)
	}

	startTime := time.Now()
	fmt.Println("Starting version cleanup...")

	err = cleanupVersions(ctx, minioClient, config.bucket, config)
	if err != nil {
		log.Printf("Error during cleanup: %v", err)
	}

	fmt.Printf("\nCleanup completed in %v\n", time.Since(startTime))
}

func processFilesFromDB(ctx context.Context, db *sql.DB, config Config, workChan chan<- FileOp, stats *Stats) {
	// Get total count first
	var totalCount int64
	err := db.QueryRow("SELECT COUNT(*) FROM files WHERE project_name = ? AND status = 'uploaded'", config.projectName).Scan(&totalCount)
	if err != nil {
		log.Printf("Error getting total count: %v", err)
		return
	}
	stats.totalInDB = totalCount

	offset := 0
	for {
		dbMutex.Lock()
		rows, err := db.QueryContext(ctx, `
			SELECT id_file, filepath, f_metadata 
			FROM files 
			WHERE project_name = ? AND status = 'uploaded'
			ORDER BY id
			LIMIT ? OFFSET ?`, config.projectName, config.batchSize, offset)
		if err != nil {
			log.Printf("Error querying files: %v", err)
			dbMutex.Unlock()
			return
		}

		count := 0
		for rows.Next() {
			var idFile, filepath, metadataStr string
			if err := rows.Scan(&idFile, &filepath, &metadataStr); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}

			var metadata FileMetadata
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
				log.Printf("Error unmarshaling metadata for %s: %v", idFile, err)
				continue
			}

			if len(idFile) < 9 {
				log.Printf("Invalid id_file format: %s", idFile)
				stats.skippedCount.Add(1)
				continue
			}

			yearMonth := idFile[2:8] // Extract YYYYMM
			sourceKey := path.Join(config.sourceFolder, idFile)
			targetKey := path.Join("download", yearMonth, path.Base(idFile))

			stats.totalObjects.Add(1)
			stats.currentBatch = fmt.Sprintf("Batch %d-%d", offset, offset+count)

			select {
			case <-ctx.Done():
				rows.Close()
				dbMutex.Unlock()
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
			count++
		}
		rows.Close()
		dbMutex.Unlock()

		if count == 0 {
			break
		}
		offset += count

		// Add a small delay to prevent database lock contention
		time.Sleep(10 * time.Millisecond)
	}
}

func worker(ctx context.Context, client *minio.Client, bucket string, workChan <-chan FileOp,
	wg *sync.WaitGroup, stats *Stats, config Config) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case work, ok := <-workChan:
			if !ok {
				return
			}

			debugLog(config, "Processing file: %s -> %s", work.sourceKey, work.targetKey)

			_, err := client.StatObject(ctx, bucket, work.sourceKey, minio.StatObjectOptions{})
			if err != nil {
				if strings.Contains(err.Error(), "The specified key does not exist") {
					logError(stats.errorLogFile, work.sourceKey, "File does not exist in MinIO")
					stats.errCount.Add(1)
					stats.processedObjects.Add(1)
					continue
				}
			}

			var lastErr error
			success := false

			for attempts := 0; attempts < config.maxRetries; attempts++ {
				if attempts > 0 {
					debugLog(config, "Retry %d for file %s", attempts, work.sourceKey)
					time.Sleep(time.Second * time.Duration(attempts))
				}

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

				debugLog(config, "Copying %s to %s", work.sourceKey, work.targetKey)
				_, err := client.CopyObject(ctx, dstOpts, srcOpts)
				if err != nil {
					lastErr = fmt.Errorf("error copying: %v", err)
					continue
				}

				// Mark for cleanup instead of removing versions here
				err = markFileForCleanup(ctx, work.sourceKey, config.dbFile)
				if err != nil {
					lastErr = fmt.Errorf("error marking for cleanup: %v", err)
					continue
				}

				success = true
				break
			}

			if !success {
				log.Printf("Failed to process %s after retries: %v", work.sourceKey, lastErr)
				logError(stats.errorLogFile, work.sourceKey, lastErr.Error())
				stats.errCount.Add(1)
			}

			stats.processedObjects.Add(1)
		}
	}
}

func checkDatabase(db *sql.DB, config Config) {
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

func showProgress(stats *Stats, totalFiles int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(
		totalFiles,
		progressbar.OptionSetDescription("Processing files..."),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(15),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

func cleanupVersions(ctx context.Context, client *minio.Client, bucket string, config Config) error {
	db, err := initDB(config.dbFile)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %v", err)
	}
	defer db.Close()

	// Get total count first
	var totalCount int
	dbMutex.Lock()
	err = db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'pending_cleanup' AND project_name = ?", config.projectName).Scan(&totalCount)
	dbMutex.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get total count: %v", err)
	}

	if totalCount == 0 {
		log.Printf("No files to cleanup for project %s", config.projectName)
		return nil
	}

	// Create cleanup script file
	timestamp := time.Now().Format("20060102_150405")
	scriptFile := fmt.Sprintf("cleanup_%s.sh", timestamp)
	f, err := os.Create(scriptFile)
	if err != nil {
		return fmt.Errorf("failed to create script file: %v", err)
	}
	defer f.Close()

	// Write script header with progress tracking functions
	f.WriteString("#!/bin/bash\n\n")
	f.WriteString("# Script to cleanup versioned objects\n")
	f.WriteString("# Generated at: " + time.Now().Format("2006-01-02 15:04:05") + "\n\n")
	f.WriteString("# Warning: This operation is destructive and cannot be undone\n")
	f.WriteString("# Make sure you have proper backups before running this script\n\n")

	// Add progress tracking functions
	f.WriteString(`# Progress tracking
start_time=$(date +%s)
total_files=` + fmt.Sprintf("%d", totalCount) + `
processed_files=0
failed_files=0
progress_file="cleanup_progress.log"

# Initialize progress file
echo "Cleanup started at: $(date)" > "$progress_file"

show_progress() {
    # Clear the previous line
    echo -ne "\033[K"
    
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    if [ $elapsed -eq 0 ]; then
        elapsed=1
    fi
    rate=$((processed_files / elapsed))
    
    # Calculate ETA
    remaining_files=$((total_files - processed_files))
    if [ $rate -gt 0 ]; then
        eta=$((remaining_files / rate))
        eta_formatted=$(date -u -d @$eta '+%H:%M:%S')
    else
        eta_formatted="Unknown"
    fi

    # Update progress file
    echo "Progress: $processed_files/$total_files ($(($processed_files * 100 / $total_files))%)" >> "$progress_file"
    echo "Failed: $failed_files" >> "$progress_file"
    echo "Rate: $rate files/sec" >> "$progress_file"
    echo "ETA: $eta_formatted" >> "$progress_file"
    echo "Last update: $(date)" >> "$progress_file"

    # Show progress in terminal
    echo -e "\rProgress: $processed_files/$total_files ($(($processed_files * 100 / $total_files))%) | Rate: $rate files/sec | ETA: $eta_formatted"
}

handle_error() {
    echo "Error processing $1" >> cleanup_errors.log
    ((failed_files++))
}

`)

	// Get files marked for cleanup
	rows, err := db.QueryContext(ctx, `
		SELECT id_file, filepath, f_metadata 
		FROM files 
		WHERE status = 'pending_cleanup' AND project_name = ?
		LIMIT ?`, config.projectName, config.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query files: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var idFile, filepath, metadataStr string
		if err := rows.Scan(&idFile, &filepath, &metadataStr); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Construct the source key using the same logic as move operation
		sourceKey := path.Join(config.sourceFolder, idFile)

		// Generate mc command with error handling and progress tracking
		cmd := fmt.Sprintf(`echo "";
mc rm --versions --force "%s/%s/%s" || handle_error "%s"
((processed_files++))
show_progress
`, 
			config.alias,
			config.bucket,
			sourceKey,
			sourceKey)
		
		_, err = f.WriteString(cmd)
		if err != nil {
			log.Printf("Error writing to script file: %v", err)
			continue
		}
		count++
	}

	// Write summary at the end of the file
	f.WriteString("\n# Show final summary\n")
	f.WriteString(`echo -e "\n\nCleanup completed!"
echo "Total processed: $processed_files"
echo "Total failed: $failed_files"
echo "Time taken: $(date -u -d @$(($(date +%s) - start_time)) '+%H:%M:%S')"
echo "See cleanup_errors.log for any errors"
`)

	fmt.Printf("\nGenerated cleanup script: %s\n", scriptFile)
	fmt.Printf("Total objects to cleanup: %d\n\n", totalCount)
	fmt.Println("To perform cleanup:")
	fmt.Println("1. Review the generated script")
	fmt.Println("2. Configure mc with your MinIO credentials:")
	fmt.Printf("   mc alias set %s %s %s %s\n", config.alias, config.endpoint, config.accessKeyID, config.secretAccessKey)
	fmt.Println("3. Run the script (it may take several hours)")
	fmt.Printf("   bash %s\n\n", scriptFile)
	fmt.Println("The script will:")
	fmt.Println("- Show real-time progress in terminal")
	fmt.Println("- Save progress to cleanup_progress.log")
	fmt.Println("- Log errors to cleanup_errors.log")
	fmt.Println("- Can be interrupted and resumed")

	return nil
}
