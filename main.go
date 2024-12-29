package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/schollz/progressbar/v3"
)

type Config struct {
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	useSSL          bool
	bucket          string
	sourceFolder    string
	baseYear        int
	workers         int
	batchSize       int
}

type FileOp struct {
	object    minio.ObjectInfo
	sourceKey string
	targetKey string
	yearMonth string
}

type Stats struct {
	totalObjects     atomic.Int64
	processedObjects atomic.Int64
	skippedCount     atomic.Int64
	errCount         atomic.Int64
}

func main() {
	config := parseFlags()

	// Initialize MinIO client
	minioClient, err := minio.New(config.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.accessKeyID, config.secretAccessKey, ""),
		Secure: config.useSSL,
	})
	if err != nil {
		log.Fatalf("Error initializing MinIO client: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel for work items
	workChan := make(chan FileOp, config.batchSize)

	// Initialize stats
	stats := &Stats{}

	// Create WaitGroup for workers
	var wg sync.WaitGroup

	// Start progress tracking
	fmt.Printf("Processing files for year %d in batches of %d...\n", config.baseYear, config.batchSize)

	// Start workers
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go worker(ctx, minioClient, config.bucket, workChan, &wg, stats)
	}

	// Create progress bar
	bar := progressbar.NewOptions64(
		-1,
		progressbar.OptionSetDescription("Processing files"),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("files"),
		progressbar.OptionSetPredictTime(true),
	)

	// Start object processing
	go func() {
		defer close(workChan)
		processObjectsInBatches(ctx, minioClient, config, workChan, stats)
	}()

	// Update progress
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				processed := stats.processedObjects.Load()
				total := stats.totalObjects.Load()
				if total > 0 {
					bar.ChangeMax64(total)
				}
				bar.Set64(processed)
				time.Sleep(time.Second)
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()

	// Final progress update
	bar.Finish()
	fmt.Printf("\nSummary for year %d:\n", config.baseYear)
	fmt.Printf("Processed: %d files\n", stats.processedObjects.Load())
	fmt.Printf("Errors: %d\n", stats.errCount.Load())
	fmt.Printf("Skipped: %d (different year or invalid format)\n", stats.skippedCount.Load())
}

func processObjectsInBatches(ctx context.Context, minioClient *minio.Client, config Config,
	workChan chan<- FileOp, stats *Stats) {
	var continuationToken string
	re := regexp.MustCompile(`U-(\d{4})(\d{2})\d+`)

	for {
		// List objects in batches
		opts := minio.ListObjectsOptions{
			Prefix:     config.sourceFolder,
			Recursive:  true,
			MaxKeys:    config.batchSize,
			StartAfter: continuationToken,
		}

		objects := minioClient.ListObjects(ctx, config.bucket, opts)

		empty := true
		for object := range objects {
			empty = false
			if object.Err != nil {
				log.Printf("Error listing objects: %v", object.Err)
				continue
			}

			continuationToken = object.Key

			matches := re.FindStringSubmatch(object.Key)
			if len(matches) < 3 {
				stats.skippedCount.Add(1)
				continue
			}

			year := matches[1]
			month := matches[2]

			// Skip if year doesn't match base year
			if year != fmt.Sprintf("%d", config.baseYear) {
				stats.skippedCount.Add(1)
				continue
			}

			yearMonth := year + month
			targetKey := path.Join("download", yearMonth, path.Base(object.Key))

			stats.totalObjects.Add(1)

			select {
			case <-ctx.Done():
				return
			case workChan <- FileOp{
				object:    object,
				sourceKey: object.Key,
				targetKey: targetKey,
				yearMonth: yearMonth,
			}:
			}
		}

		// If no objects were returned, we've reached the end
		if empty {
			break
		}

		// Log progress for each batch
		log.Printf("Processed batch up to: %s", continuationToken)
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

			// Copy object to new location with updated metadata
			srcOpts := minio.CopySrcOptions{
				Bucket: bucket,
				Object: work.sourceKey,
			}

			// Get existing metadata
			objInfo, err := client.StatObject(ctx, bucket, work.sourceKey, minio.StatObjectOptions{})
			if err != nil {
				log.Printf("Error getting object metadata for %s: %v", work.sourceKey, err)
				stats.errCount.Add(1)
				continue
			}

			// Update bucket metadata
			userMeta := objInfo.UserMetadata
			userMeta["X-Amz-Meta-Bucket"] = path.Join(bucket, work.targetKey)

			dstOpts := minio.CopyDestOptions{
				Bucket:          bucket,
				Object:          work.targetKey,
				UserMetadata:    userMeta,
				ReplaceMetadata: true,
			}

			// Copy object with new metadata
			_, err = client.CopyObject(ctx, dstOpts, srcOpts)
			if err != nil {
				log.Printf("Error copying object %s to %s: %v", work.sourceKey, work.targetKey, err)
				stats.errCount.Add(1)
				continue
			}

			// Remove old object
			err = client.RemoveObject(ctx, bucket, work.sourceKey, minio.RemoveObjectOptions{})
			if err != nil {
				log.Printf("Error removing old object %s: %v", work.sourceKey, err)
				stats.errCount.Add(1)
				continue
			}

			stats.processedObjects.Add(1)
		}
	}
}

func parseFlags() Config {
	endpoint := flag.String("endpoint", "", "MinIO endpoint (required)")
	accessKey := flag.String("access-key", "", "Access key (required)")
	secretKey := flag.String("secret-key", "", "Secret key (required)")
	useSSL := flag.Bool("use-ssl", true, "Use SSL for connection")
	bucket := flag.String("bucket", "", "Target bucket (required)")
	sourceFolder := flag.String("source-folder", "download", "Source folder")
	baseYear := flag.Int("base-year", 0, "Base year to process files from (required)")
	workers := flag.Int("workers", 10, "Number of worker goroutines")
	batchSize := flag.Int("batch-size", 1000, "Number of objects to list per batch")

	flag.Parse()

	// Validate required flags
	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" || *baseYear == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Validate year
	if *baseYear < 2000 || *baseYear > 2100 {
		log.Fatal("Base year must be between 2000 and 2100")
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
		useSSL:          *useSSL,
		bucket:          *bucket,
		sourceFolder:    cleanSourceFolder,
		baseYear:        *baseYear,
		workers:         *workers,
		batchSize:       *batchSize,
	}
}
