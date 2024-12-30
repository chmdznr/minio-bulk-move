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
	alias           string
}

type FileOp struct {
	sourceKey string
	targetKey string
	yearMonth string
	metadata  map[string]string
}

type Stats struct {
	processedObjects atomic.Int64
	errCount        atomic.Int64
	errorLogFile    *os.File
	successFiles    []string
	failedFiles     []string
	statsMutex      sync.Mutex
	startTime       time.Time
	totalFiles      int64
	lastError       atomic.Value
}

func NewStats(errorLogFile *os.File, totalFiles int64) *Stats {
	s := &Stats{
		errorLogFile: errorLogFile,
		successFiles: make([]string, 0, totalFiles),
		failedFiles:  make([]string, 0),
		startTime:    time.Now(),
		totalFiles:   totalFiles,
	}
	s.lastError.Store("")
	return s
}

func (s *Stats) AddSuccessFile(file string) {
	s.statsMutex.Lock()
	s.successFiles = append(s.successFiles, file)
	s.statsMutex.Unlock()
	s.processedObjects.Add(1)
}

func (s *Stats) AddFailedFile(file string) {
	s.statsMutex.Lock()
	s.failedFiles = append(s.failedFiles, file)
	s.statsMutex.Unlock()
	s.errCount.Add(1)
	s.processedObjects.Add(1)
}

func displayProgress(stats *Stats, bar *progressbar.ProgressBar) {
	processed := stats.processedObjects.Load()
	errors := stats.errCount.Load()
	total := stats.totalFiles
	elapsed := time.Since(stats.startTime).Seconds()
	speed := float64(processed) / elapsed

	remaining := total - processed - errors
	eta := time.Duration(float64(remaining) / speed) * time.Second

	percentage := float64(processed+errors) / float64(total) * 100

	var lastError string
	if stats.lastError.Load() != "" {
		lastError = fmt.Sprintf(" | Last Error: %s", stats.lastError.Load())
	}

	description := fmt.Sprintf("\rProgress: %.1f%% | Success: %d | Failed: %d | Speed: %.0f/s | ETA: %v%s",
		percentage,
		processed,
		errors,
		speed,
		eta.Round(time.Second),
		lastError)

	bar.Describe(description)
	bar.Set64(processed + errors)
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
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	_, err := fmt.Fprintf(f, "[%s] %s: %s\n", timestamp, sourceKey, errMsg)
	if err != nil {
		log.Printf("Error writing to error log: %v", err)
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

func worker(ctx context.Context, client *minio.Client, bucket string, workChan <-chan FileOp,
	wg *sync.WaitGroup, stats *Stats, config Config) {
	defer wg.Done()

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
	}

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure:    config.useSSL,
		Transport: transport,
	})
	if err != nil {
		log.Printf("Error creating MinIO client in worker: %v", err)
		stats.lastError.Store(fmt.Sprintf("MinIO Error: %v", err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case work, ok := <-workChan:
			if !ok {
				return
			}

			debugLog(config, "Processing file: %s -> %s", work.sourceKey, work.targetKey)

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
				opCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				startTime := time.Now()
				_, err := minioClient.CopyObject(opCtx, dstOpts, srcOpts)
				elapsed := time.Since(startTime)
				cancel()

				if err != nil {
					if strings.Contains(err.Error(), "The specified key does not exist") {
						logError(stats.errorLogFile, work.sourceKey, "File does not exist in MinIO")
						stats.AddFailedFile(work.sourceKey)
						stats.lastError.Store(fmt.Sprintf("Not Found: %s", work.sourceKey))
						success = true // Mark as success to avoid retries
						break
					}
					lastErr = fmt.Errorf("error copying (took %v): %v", elapsed, err)
					debugLog(config, "Copy error for %s: %v", work.sourceKey, err)
					stats.lastError.Store(fmt.Sprintf("Copy Error: %v", err))
					continue
				}
				debugLog(config, "Copy successful for %s (took %v)", work.sourceKey, elapsed)

				stats.AddSuccessFile(work.sourceKey)
				success = true
				break
			}

			if !success {
				log.Printf("Failed to process %s after retries: %v", work.sourceKey, lastErr)
				logError(stats.errorLogFile, work.sourceKey, lastErr.Error())
				stats.AddFailedFile(work.sourceKey)
				stats.lastError.Store(fmt.Sprintf("Failed: %v", lastErr))
			}
		}
	}
}

func updateDatabase(ctx context.Context, successFiles []string, dbFile string) error {
	if len(successFiles) == 0 {
		return nil
	}

	log.Printf("Starting database update for %d files...", len(successFiles))
	startTime := time.Now()

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	// Set database optimizations
	log.Printf("Setting database optimizations...")
	if _, err := db.Exec(`
		PRAGMA busy_timeout = 30000;
		PRAGMA synchronous = OFF;
		PRAGMA journal_mode = MEMORY;
		PRAGMA temp_store = MEMORY;
		PRAGMA cache_size = -2000000;
	`); err != nil {
		return fmt.Errorf("error setting database optimizations: %v", err)
	}

	// Update in batches of 1000
	batchSize := 1000
	totalBatches := (len(successFiles) + batchSize - 1) / batchSize
	log.Printf("Processing %d batches of %d files each...", totalBatches, batchSize)

	for i := 0; i < len(successFiles); i += batchSize {
		batchStart := time.Now()
		end := i + batchSize
		if end > len(successFiles) {
			end = len(successFiles)
		}

		// Create a batch query with multiple values
		placeholders := make([]string, end-i)
		args := make([]interface{}, end-i)
		for j := 0; j < end-i; j++ {
			placeholders[j] = "?"
			args[j] = path.Base(successFiles[i+j])
		}

		query := fmt.Sprintf("UPDATE files SET status = 'pending_cleanup' WHERE id_file IN (%s)",
			strings.Join(placeholders, ","))

		// Execute the batch update
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("error updating batch: %v", err)
		}

		batchTime := time.Since(batchStart)
		progress := float64(end) / float64(len(successFiles)) * 100
		log.Printf("Batch %d/%d (%.1f%%) completed in %v", (i/batchSize)+1, totalBatches, progress, batchTime)
	}

	totalTime := time.Since(startTime)
	log.Printf("Database update completed in %v", totalTime)
	return nil
}

func runMove(config Config) error {
	debugLog(config, "Starting move operation with config: %+v", config)

	// Validate required fields
	if config.endpoint == "" || config.accessKeyID == "" || config.secretAccessKey == "" ||
		config.bucket == "" || config.sourceFolder == "" || config.dbFile == "" || config.projectName == "" {
		return fmt.Errorf("all parameters are required")
	}

	// Initialize MinIO client
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
	}

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure:    config.useSSL,
		Transport: transport,
	})
	if err != nil {
		return fmt.Errorf("error creating MinIO client: %v", err)
	}
	debugLog(config, "MinIO client initialized successfully")

	// Create error log file
	errorLogFile, err := createErrorLogFile(config.projectName)
	if err != nil {
		return fmt.Errorf("error creating error log file: %v", err)
	}
	defer errorLogFile.Close()

	// Initialize stats
	dbPool, err := initDB(config.dbFile)
	if err != nil {
		return fmt.Errorf("error initializing database: %v", err)
	}
	var totalCount int64
	err = dbPool.QueryRow("SELECT COUNT(*) FROM files WHERE project_name = ? AND status = 'uploaded'", config.projectName).Scan(&totalCount)
	if err != nil {
		return fmt.Errorf("error getting total count: %v", err)
	}
	stats := NewStats(errorLogFile, totalCount)

	// Initialize context
	ctx := context.Background()

	// Initialize database
	defer dbPool.Close()
	debugLog(config, "Database initialized successfully")

	// Initialize work channel and wait group
	workChan := make(chan FileOp, config.batchSize)
	var wg sync.WaitGroup

	// Start progress bar
	bar := showProgress(stats, totalCount)
	defer bar.Close()

	// Start progress updater
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				displayProgress(stats, bar)
			}
		}
	}()

	// Start workers
	debugLog(config, "Starting %d workers", config.workers)
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go worker(ctx, minioClient, config.bucket, workChan, &wg, stats, config)
	}

	// Process files from database
	go func() {
		defer close(workChan)
		processFilesFromDB(ctx, dbPool, config, workChan, stats)
	}()

	// Wait for all workers to finish
	debugLog(config, "Waiting for workers to finish")
	wg.Wait()

	// Update progress one last time
	displayProgress(stats, bar)

	// Update database with successful files
	log.Printf("\nCopy operations completed. Starting database updates...")
	log.Printf("Total files processed: %d", stats.processedObjects.Load())
	log.Printf("Successful copies: %d", len(stats.successFiles))
	log.Printf("Failed copies: %d", len(stats.failedFiles))

	if err := updateDatabase(ctx, stats.successFiles, config.dbFile); err != nil {
		return fmt.Errorf("error updating database: %v", err)
	}

	// Log failed files
	if len(stats.failedFiles) > 0 {
		log.Printf("\nFailed to process %d files:", len(stats.failedFiles))
		for _, file := range stats.failedFiles {
			log.Printf("  - %s", file)
		}
	}

	log.Printf("\nMove operation completed successfully!")
	log.Printf("Total files: %d", totalCount)
	log.Printf("Processed: %d", stats.processedObjects.Load())
	log.Printf("Errors: %d", stats.errCount.Load())
	log.Printf("Total time: %v", time.Since(stats.startTime))

	return nil
}

func showProgress(stats *Stats, totalFiles int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(
		totalFiles,
		progressbar.OptionSetDescription("Processing files..."),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(100),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
	)
}

func processFilesFromDB(ctx context.Context, db *sql.DB, config Config, workChan chan<- FileOp, stats *Stats) {
	offset := 0
	for {
		rows, err := db.QueryContext(ctx, `
			SELECT id_file, filepath, f_metadata 
			FROM files 
			WHERE project_name = ? AND status = 'uploaded'
			ORDER BY id
			LIMIT ? OFFSET ?`, config.projectName, config.batchSize, offset)
		if err != nil {
			log.Printf("Error querying files: %v", err)
			return
		}
		defer rows.Close()

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
				stats.errCount.Add(1)
				continue
			}

			yearMonth := idFile[2:8] // Extract YYYYMM
			sourceKey := path.Join(config.sourceFolder, idFile)
			targetKey := path.Join("download", yearMonth, path.Base(idFile))

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
			count++
		}

		if count == 0 {
			break
		}
		offset += count

		// Add a small delay to prevent database lock contention
		time.Sleep(10 * time.Millisecond)
	}
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
	moveCmd.StringVar(&moveConfig.alias, "alias", "", "MinIO alias for mc command")
	moveCmd.BoolVar(&moveConfig.debug, "debug", false, "Enable debug logging")

	cleanupConfig := Config{}
	cleanupCmd.StringVar(&cleanupConfig.bucket, "bucket", "", "MinIO bucket name")
	cleanupCmd.StringVar(&cleanupConfig.sourceFolder, "source-folder", "", "Source folder path in bucket")
	cleanupCmd.IntVar(&cleanupConfig.batchSize, "batch-size", 1000, "Number of files to process per batch")
	cleanupCmd.StringVar(&cleanupConfig.dbFile, "db-file", "", "Path to SQLite database file")
	cleanupCmd.StringVar(&cleanupConfig.projectName, "project-name", "", "Project name to process from database")
	cleanupCmd.StringVar(&cleanupConfig.alias, "alias", "", "MinIO alias for mc command")
	cleanupCmd.BoolVar(&cleanupConfig.debug, "debug", false, "Enable debug logging")

	if len(os.Args) < 2 {
		fmt.Println("Expected 'move' or 'cleanup' subcommands")
		fmt.Println("\nUsage:")
		fmt.Println("  move     - Move files and mark for version cleanup")
		fmt.Println("  cleanup  - Clean up versions of previously moved files")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "move":
		moveCmd.Parse(os.Args[2:])
		if err := runMove(moveConfig); err != nil {
			log.Fatal(err)
		}
	case "cleanup":
		cleanupCmd.Parse(os.Args[2:])
		if err := runCleanup(cleanupConfig); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}
}

func runCleanup(config Config) error {
	// Validate cleanup command flags
	if config.endpoint == "" || config.accessKeyID == "" || config.secretAccessKey == "" ||
		config.bucket == "" || config.dbFile == "" || config.sourceFolder == "" || config.projectName == "" ||
		config.alias == "" {
		return fmt.Errorf("all parameters are required")
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure: config.useSSL,
	})
	if err != nil {
		return fmt.Errorf("error creating MinIO client: %v", err)
	}

	startTime := time.Now()
	fmt.Println("Starting version cleanup...")

	err = cleanupVersions(ctx, minioClient, config.bucket, config)
	if err != nil {
		return fmt.Errorf("error during cleanup: %v", err)
	}

	fmt.Printf("\nCleanup completed in %v\n", time.Since(startTime))
	return nil
}

func cleanupVersions(ctx context.Context, client *minio.Client, bucket string, config Config) error {
	db, err := initDB(config.dbFile)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %v", err)
	}
	defer db.Close()

	// Get total count first
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'pending_cleanup' AND project_name = ?", config.projectName).Scan(&totalCount)
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

type FileMetadata struct {
	ExistingID string `json:"existing_id"`
	IDProfile  string `json:"id_profile"`
	NamaFile   string `json:"nama_file_asli"`
	NamaModul  string `json:"nama_modul"`
}
