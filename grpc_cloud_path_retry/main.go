// Sample storage-quickstart creates a Google Cloud Storage bucket using
// gRPC API.
package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
)

func ShouldRetry(err error) bool {
	if storage.ShouldRetry(err) {
		log.Printf("Retrying for error: %v", err)
		return true
	}
	return false
}

func main() {
	ctx := context.Background()

	// Use your Google Cloud Platform project ID and Cloud Storage bucket
	projectID := "gcs-tess"
	bucketName := "princer-ssiog-data-bkt-uc1"
	objectName := "12G/experiment.0"
	

	// Creates a gRPC enabled client.
	client, err := storage.NewGRPCClient(ctx)
	client.SetRetry(storage.WithErrorFunc(ShouldRetry), storage.WithMaxAttempts(5))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Creates the new bucket.
	if err := client.Bucket(bucketName).Create(ctx, projectID, nil); err != nil {
		log.Printf("Failed to create bucket: %v", err)
	}

	// Create array of reader and close in the last

	// Create an array of *Reader
	readers := make([]*storage.Reader, 0)
	for i := 0; i < 1025; i++ {
		rc, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
		if err != nil {
			log.Fatalf("Failed to create reader: %v", err)
		}
		readers = append(readers, rc)
		buf := make([]byte, 1)
		_, err = rc.Read(buf)
		if err != nil {
			log.Fatalf("Failed to read from object: %v", err)
		}
		if i % 10 == 0 {
			log.Printf("Finished reading from %d object", i)
		}
	}
	log.Printf("Finished reading from object")

	for _, reader := range readers {
		reader.Close()
	}

	fmt.Printf("Bucket %v created.\n", bucketName)	
}