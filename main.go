package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"

	pb "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"log"
	"os"
	"sync"
)

const (
	spicedbEndpoint = "192.168.1.86:50051"
	token           = "team132rocks"
)

var client *authzed.Client

// Worker function to handle a batch of records
func processBatch(batchNum int, records [][]string, wg *sync.WaitGroup) {
	defer wg.Done()

	updates := []*pb.RelationshipUpdate{}

	for _, record := range records {
		col1 := record[0]
		col2 := record[1]

		updates = append(updates, &pb.RelationshipUpdate{
			Operation: pb.RelationshipUpdate_OPERATION_CREATE,
			Relationship: &pb.Relationship{
				Resource: &pb.ObjectReference{
					ObjectType: "node",
					ObjectId:   col1,
				},
				Relation: "edge",
				Subject: &pb.SubjectReference{
					Object: &pb.ObjectReference{
						ObjectType: "node",
						ObjectId:   col2,
					},
				},
			},
		})
	}

	request := &pb.WriteRelationshipsRequest{Updates: updates}
	_, err := client.WriteRelationships(context.Background(), request)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Finished processing batch %d with %d records\n", batchNum, len(records))
}

// Function to read CSV and start processing in batches
func processCSVInBatches(filename string, batchSize int) {
	// Open the CSV file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = ','

	var batch [][]string
	var wg sync.WaitGroup
	batchNum := 0

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		// Add to current batch
		batch = append(batch, record)

		// When batch is full, start a goroutine to process it
		if len(batch) >= batchSize {
			batchNum++
			wg.Add(1)
			go processBatch(batchNum, batch, &wg)
			batch = nil // Reset batch
		}
	}

	// Process any remaining records in the final batch
	if len(batch) > 0 {
		batchNum++
		wg.Add(1)
		go processBatch(batchNum, batch, &wg)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Println("All batches processed.")
}

func main() {
	var err error
	client, err = authzed.NewClient(
		spicedbEndpoint,
		grpcutil.WithInsecureBearerToken(token),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("unable to initialize client: %s", err)
	}

	// Call the function with the filename and batch size
	processCSVInBatches("../datasets/edges_25k.csv", 1000)
}
