package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

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

	numOfWorker = flag.Int("worker", 128, "Number of concurrent worker to read")

	runTime = flag.Duration("run-time", 3*time.Minute, "Actual workload runtime")

	warmUpTime = flag.Duration("warm-up-time", 2*time.Second, "Ramp up time")

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
			//fmt.Printf("Ramping up for idx: %d\n", idx)
			eG.Go(func() error {
				_, err := ReadObject(warmupCtx, idx, bucketHandle)
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
func ReadObject(ctx context.Context, workerID int, bucketHandle *storage.BucketHandle) (bytesRead int64, err error) {
	objectName := *objectNamePrefix + strconv.Itoa(workerID) + *objectNameSuffix

	select {
	case <-ctx.Done():
		return 0, nil
	default:
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return 0, fmt.Errorf("while creating reader object: %v", err)
		}
		defer rc.Close()

		// Calls Reader.WriteTo implicitly.
		count, err := io.Copy(io.Discard, rc)
		bytesRead += count
		if err != nil {
			return bytesRead, fmt.Errorf("while reading and discarding content: %v", err)
		}
		return bytesRead, nil
	}
}

func main() {
	flag.Parse()
	fmt.Printf("Workload start time: %s\n", time.Now().String())
	ctx := context.Background()

	var client *storage.Client
	var err error
	protocol := ""
	if *clientProtocol == "http" {
		protocol = "http"
		client, err = CreateHTTPClient(ctx, false)
	} else {
		protocol = "grpc"
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
	//fmt.Println("Ramp-up starts")

	rampUp(warmupCtx, cancelFn, bucketHandle)

	//fmt.Println("Ramp-up complete. Starting run on actual traffic.")
	startTime := time.Now()
	var eG errgroup.Group

	actualRunCtx, cancelFn := context.WithDeadline(ctx, startTime.Add(*runTime))
	defer cancelFn()

	// Run the actual workload
	for i := 0; i < *numOfWorker; i++ {
		idx := i
		eG.Go(func() error {
			//fmt.Printf("Worker %d started\n", idx)
			for {
				select {
				case <-actualRunCtx.Done():
					return nil
				default:
					bytesRead, _ := ReadObject(actualRunCtx, idx, bucketHandle)
					totalBytesRead.Add(bytesRead)
				}
			}
		})
	}

	err = eG.Wait()
	totalDuration := time.Since(startTime)

	if err == nil && err != context.DeadlineExceeded {
		fmt.Printf("Protocol: %s, Bandwidth: %d MiB/s\n", protocol, totalBytesRead.Load()/(int64(totalDuration.Seconds())*MiB))
		fmt.Printf("Workload end time: %s\n\n", time.Now().String())
		os.Exit(0)
	} else {
		fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
		fmt.Printf("Workload end time: %s\n\n", time.Now().String())
		os.Exit(1)
	}
}
