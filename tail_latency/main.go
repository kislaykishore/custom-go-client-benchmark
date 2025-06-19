package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	maxConnsPerHost     = 0
	maxIdleConnsPerHost = 100

	// MiB means 1024 KiB.
	MiB = int64(1024 * 1024)

	grpcConnPoolSize = flag.Int("grpc-conn-pool-size", 1, "grpc connection pool size")
	numOfWorkers     = flag.Int("worker", 128, "Number of concurrent worker to read")

	maxRetryDuration = 30 * time.Second

	retryMultiplier = 2.0

	bucketName = flag.String("bucket", "kislayk_europe_west4", "GCS bucket name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")
)

// CreateHTTPClient create http storage client.
func CreateHTTPClient(ctx context.Context) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	transport = &http.Transport{
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// This disables HTTP/2 in transport.
		TLSNextProto: make(
			map[string]func(string, *tls.Conn) http.RoundTripper,
		),
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &userAgentRoundTripper{
			wrapped: &oauth2.Transport{
				Base:   transport,
				Source: tokenSource,
			},
			UserAgent: "prince",
		},
		Timeout: 0,
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

// CreateGrpcClient creates grpc client.
func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(*grpcConnPoolSize), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics())
}

func getClient(ctx context.Context, clientProtocol string) (client *storage.Client, err error) {
	if clientProtocol == "http" {
		client, err = CreateHTTPClient(ctx)
	} else {
		client, err = CreateGrpcClient(ctx)
	}
	if err != nil {
		return nil, err
	}
	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        maxRetryDuration,
			Multiplier: retryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))
	return client, nil
}

type objMeta struct {
	name string
	size int64
}

type task struct {
	id     int64
	block  Block
	name   string
	offset int64
	length int64
}

func main() {
	flag.Parse()
	fmt.Printf("Workload start time: %s\n", time.Now().String())
	fmt.Printf("Number of workers: %d\n", *numOfWorkers)
	fmt.Printf("Bucket name: %s\n", *bucketName)
	fmt.Printf("Client protocol: %s\n", *clientProtocol)
	ctx := context.Background()
	client, err := getClient(ctx, *clientProtocol)
	if err != nil {
		fmt.Printf("while creating the client: %v", err)
		os.Exit(1)
	}

	// assumes bucket already exist
	bucketHandle := client.Bucket(*bucketName)
	itr := bucketHandle.Objects(ctx, nil)
	allObjects := make([]objMeta, 0)
	for {
		objAttrs, err := itr.Next()
		if err == iterator.Done {
			break
		}
		if objAttrs.Size == 0 {
			// Skip
			continue
		}
		allObjects = append(allObjects, objMeta{name: objAttrs.Name, size: objAttrs.Size})
	}

	// Create tasks
	tasks := make([]task, 0, 1024)
	partitionSize := 200 * MiB
	for idx, obj := range allObjects {
		// Break into tasks
		for offset := int64(0); offset < obj.size; offset += partitionSize {
			length := partitionSize
			if obj.size-offset < partitionSize {
				length = obj.size - offset
			}
			tasks = append(tasks, task{id: int64(idx), offset: offset, length: length, name: obj.name})
		}
	}

	ch := make(chan task)

	fmt.Printf("Number of tasks identified: %d\n", len(tasks))

	for idx := range tasks {
		var err error
		if tasks[idx].block, err = createBlock(tasks[idx].length); err != nil {
			panic(err)
		}
	}
	go func() {
		for _, t := range tasks {
			ch <- t
		}
		close(ch)
	}()

	timeTaken := make([]int64, len(tasks))
	st := time.Now()
	processTasks(ctx, ch, timeTaken, bucketHandle)

	totalReadTimeTaken := time.Since(st).Nanoseconds()

	// Once all returned, compute percentiles
	slices.Sort(timeTaken)

	fmt.Printf("P50: %d ms\n", convertNanosToMillis(timeTaken[len(timeTaken)/2]))
	fmt.Printf("P90: %d ms\n", convertNanosToMillis(timeTaken[len(timeTaken)*9/10]))
	fmt.Printf("P95: %d ms\n", convertNanosToMillis(timeTaken[len(timeTaken)*95/100]))
	fmt.Printf("P99: %d ms\n", convertNanosToMillis(timeTaken[len(timeTaken)*99/100]))
	fmt.Printf("P100: %d ms\n", convertNanosToMillis(timeTaken[len(timeTaken)-1]))

	totalRead := int64(0)
	for _, t := range tasks {
		if t.block == nil {
			panic("Block is nil for some reason")
		}
		rd := t.block.Reader()
		n, _ := io.Copy(io.Discard, rd)
		totalRead += int64(n)
	}
	fmt.Printf("Data read: %d bytes\n", totalRead)
	fmt.Printf("Total time taken: %d ms\n", convertNanosToMillis(totalReadTimeTaken))
	fmt.Printf("Throughput: %.2f GiB/sec\n", (float64(totalRead) / float64(1024*1024*1024*convertNanosToSeconds(totalReadTimeTaken))))
}

func processTasks(ctx context.Context, ch chan task, timeTaken []int64, bucketHandle *storage.BucketHandle) {
	var wg sync.WaitGroup
	for i := 0; i < *numOfWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, 2*MiB)
			for {
				t, ok := <-ch
				if !ok {
					return
				}
				// Process task
				obj := bucketHandle.Object(t.name)
				startTime := time.Now()
				reader, err := obj.NewRangeReader(ctx, t.offset, t.length)
				timeTaken[t.id] = time.Since(startTime).Nanoseconds()
				if err != nil {
					panic(err)
				}
				defer reader.Close()
				for {
					n, err := reader.Read(buffer)
					t.block.Write(buffer[:n])
					if err == io.EOF {
						break
					}
					if err != nil {
						panic(err)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func convertNanosToMillis(t int64) int64 {
	return t / 1000000
}
func convertNanosToSeconds(t int64) int64 {
	return t / 1000000000
}
