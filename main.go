package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"

	// Register the pprof endpoints under the web server root at /debug/pprof
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/profiler"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

var (
	grpcConnPoolSize    = 1
	maxConnsPerHost     = 100
	maxIdleConnsPerHost = 0

	// MB means 1024 Kb.
	MiB = int64(1024 * 1024)

	numOfWorker = flag.Int("worker", 1, "Number of concurrent worker to read")

	numOfReadCallPerWorker = flag.Int("read-call-per-worker", 100000, "Number of read call per worker")

	warmUpTime = flag.Duration("warm-up-time", 2*time.Minute, "Ramp up time")

	maxRetryDuration = 30 * time.Second

	retryMultiplier = 2.0

	bucketName = flag.String("bucket", "kislayk_europe_west4", "GCS bucket name.")

	// ProjectName denotes gcp project name.
	ProjectName = flag.String("project", "tpu-prod-env-one-vm", "GCP project name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	// Object name = objectNamePrefix + {thread_id} + objectNameSuffix
	objectNamePrefix = flag.String("obj-prefix", "1GB/experiment.", "Object prefix")
	objectNameSuffix = flag.String("obj-suffix", ".0", "Object suffix")

	tracerName      = "kislayk-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	enablePprof     = flag.Bool("enable-pprof", false, "Enable pprof server")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")

	// Cloud profiler.
	enableCloudProfiler = flag.Bool("enable-cloud-profiler", false, "Enable cloud profiler")
	enableHeap          = flag.Bool("heap", false, "enable heap profile collection")
	enableCPU           = flag.Bool("cpu", true, "enable cpu profile collection")
	enableHeapAlloc     = flag.Bool("heap_alloc", false, "enable heap allocation profile collection")
	enableThread        = flag.Bool("thread", false, "enable thread profile collection")
	enableContention    = flag.Bool("contention", false, "enable contention profile collection")
	projectID           = flag.String("project_id", "", "project ID to run profiler with; only required when running outside of GCP.")
	version             = flag.String("version", "original", "version to run profiler with")

	// Enable read stall retry.
	enableReadStallRetry = flag.Bool("enable-read-stall-retry", false, "Enable read stall retry")
)

// CreateHTTPClient create http storage client.
func CreateHTTPClient(ctx context.Context, isHTTP2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if !isHTTP2 {
		transport = &http.Transport{
			MaxConnsPerHost:     maxConnsPerHost,
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   maxConnsPerHost,
			ForceAttemptHTTP2: true,
		}
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSource,
		},
		Timeout: 0,
	}

	// Setting UserAgent through RoundTripper middleware
	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: "prince",
	}

	if *enableReadStallRetry {
		return storage.NewClient(ctx, option.WithHTTPClient(httpClient),
			experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
				Min:              time.Second,
				TargetPercentile: 0.99,
			}))
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func rampUp(warmupCtx context.Context, cancelFn context.CancelFunc, bucketHandle *storage.BucketHandle) {
	idx := 0
	var eG errgroup.Group
	for {
		select {
		case <-warmupCtx.Done():
			cancelFn()
			return
		default:
			if idx == *numOfWorker {
				fmt.Println("Waiting")
				eG.Wait()
				return
			}
			time.Sleep(1 * time.Second)
			fmt.Printf("Ramping up for idx: %d\n", idx)
			eG.Go(func() error {
				_, err := ReadObject(warmupCtx, idx, -1, bucketHandle)
				if err != nil {
					err = fmt.Errorf("while reading object %v: %w", *objectNamePrefix+strconv.Itoa(idx)+*objectNameSuffix, err)
					return err
				}
				return err
			})
			idx++
		}
	}
}

// CreateGrpcClient creates grpc client.
func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(grpcConnPoolSize), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics())
}

// ReadObject creates reader object corresponding to workerID with the help of bucketHandle.
func ReadObject(ctx context.Context, workerID int, numCalls int64, bucketHandle *storage.BucketHandle) (bytesRead int64, err error) {
	objectName := *objectNamePrefix + strconv.Itoa(workerID) + *objectNameSuffix

	for i := int64(0); i < numCalls || numCalls == -1; i++ {
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return 0, fmt.Errorf("while creating reader object: %v", err)
		}

		// Calls Reader.WriteTo implicitly.
		count, err := io.Copy(io.Discard, rc)
		rc.Close()
		bytesRead += count
		if err != nil {
			return 0, fmt.Errorf("while reading and discarding content: %v", err)
		}
	}

	return bytesRead, nil
}

func main() {
	flag.Parse()
	ctx := context.Background()

	if *enableTracing {
		cleanup := enableTraceExport(ctx, *traceSampleRate)
		defer cleanup()
	}

	// Start a pprof server.
	// Example usage (run the following command while the script is running):
	// go tool pprof http://localhost:8080/debug/pprof/profile?seconds=60
	if *enablePprof {
		go func() {
			if err := http.ListenAndServe("localhost:8080", nil); err != nil {
				log.Fatalf("error starting http server for pprof: %v", err)
			}
		}()
	}

	if *enableCloudProfiler {
		if err := profiler.Start(profiler.Config{
			Service:              "custom-go-benchmark",
			ServiceVersion:       *version,
			ProjectID:            *projectID,
			NoCPUProfiling:       !*enableCPU,
			NoHeapProfiling:      !*enableHeap,
			NoAllocProfiling:     !*enableHeapAlloc,
			NoGoroutineProfiling: !*enableThread,
			MutexProfiling:       *enableContention,
			DebugLogging:         true,
		}); err != nil {
			log.Fatalf("Failed to start profiler: %v", err)
		}
	}

	var client *storage.Client
	var err error
	if *clientProtocol == "http" {
		client, err = CreateHTTPClient(ctx, false)
	} else {
		client, err = CreateGrpcClient(ctx)
	}

	if err != nil {
		fmt.Printf("while creating the client: %v", err)
		os.Exit(1)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        maxRetryDuration,
			Multiplier: retryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*bucketName)

	var totalBytesRead atomic.Int64

	warmupCtx, cancelFn := context.WithDeadline(ctx, time.Now().Add(*warmUpTime))
	fmt.Println("Ramp-up starts")

	rampUp(warmupCtx, cancelFn, bucketHandle)

	fmt.Println("Ramp-up complete. Starting run on actual traffic.")
	startTime := time.Now()
	var eG errgroup.Group

	// Run the actual workload
	for i := 0; i < *numOfWorker; i++ {
		idx := i
		eG.Go(func() error {
			bytesRead, err := ReadObject(ctx, idx, int64(*numOfReadCallPerWorker), bucketHandle)
			if err != nil {
				err = fmt.Errorf("while reading object %v: %w", *objectNamePrefix+strconv.Itoa(idx)+*objectNameSuffix, err)
				return err
			}
			fmt.Printf("Worker returned after reading %d bytes\n", bytesRead)
			totalBytesRead.Add(bytesRead)
			return err
		})
	}

	err = eG.Wait()
	totalDuration := time.Since(startTime)

	if err == nil {
		fmt.Printf("Read benchmark completed successfully! Bandwidth: %d MiB/s\n", totalBytesRead.Load()/(int64(totalDuration.Seconds())*MiB))
		os.Exit(0)
	} else {
		fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
		os.Exit(1)
	}
}
