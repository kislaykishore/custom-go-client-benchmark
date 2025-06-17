package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
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

func main() {
	flag.Parse()
	fmt.Printf("Workload start time: %s\n", time.Now().String())
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
	fmt.Printf("Number of objects: %d\n", len(allObjects))

	// Create a reader with the concurrency specified and measure the tail latency.
	timeTaken := make([]int64, len(allObjects))
	var wg sync.WaitGroup
	wg.Add(len(allObjects))
	for idx, obj := range allObjects {
		go func() {
			defer wg.Done()
			startTime := time.Now()
			reader, err := bucketHandle.Object(obj.name).NewReader(ctx)
			if err != nil {
				panic(err)
			}
			defer reader.Close()
			elapsedTime := time.Since(startTime)
			timeTaken[idx] = elapsedTime.Nanoseconds()
		}()
	}
	wg.Wait()

	// Once all returned, compute percentiles
	sort.Slice(timeTaken, func(i, j int) bool {
		return timeTaken[i] < timeTaken[j]
	})

	fmt.Printf("P50: %d\n", convertNanosToMillis(timeTaken[len(timeTaken)/2]))
	fmt.Printf("P90: %d\n", convertNanosToMillis(timeTaken[len(timeTaken)*9/10]))
	fmt.Printf("P95: %d\n", convertNanosToMillis(timeTaken[len(timeTaken)*95/100]))
	fmt.Printf("P99: %d\n", convertNanosToMillis(timeTaken[len(timeTaken)*99/100]))
	fmt.Printf("P100: %d\n", convertNanosToMillis(timeTaken[len(timeTaken)-1]))
}

func convertNanosToMillis(t int64) int64 {
	return t / 1000000
}
