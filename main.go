package main

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/claudiunicolaa/natsdedup"
	"github.com/nats-io/nats.go"
)

const (
	// Input subject
	subjectIn = "input"
	// Output subject
	subjectOut = "output"
	// TTL for the deduplicator
	ttl = 1 * time.Minute
	// Number of items to publish
	size = 100000
)

type item struct {
	ID  int    `json:"id"`
	Key string `json:"key"`
	Val string `json:"val"`
}

func main() {
	// Connect to a NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	// Create a deduplicator
	dedup := natsdedup.NewDeduplicator(subjectIn, subjectOut, ttl)
	if err != nil {
		log.Fatal(err)
	}

	// Start the deduplicator in a goroutine
	log.Println("Starting deduplicator...")
	go dedup.Run(nc)
	log.Println("Deduplicator started")

	// Create a list of items to deduplicate
	log.Println("Creating items...")
	items := dataSetup(size)
	log.Printf("Created %d items\n", size)

	// Start a subscriber to the output subject in a goroutine
	log.Println("Starting subscriber to output subject...")
	var storage []*item
	go func() {
		_, err := nc.Subscribe(subjectOut, func(m *nats.Msg) {
			var newItem *item
			err := json.Unmarshal(m.Data, &newItem)
			if err != nil {
				log.Fatal(err)
			}
			storage = append(storage, newItem)
		})
		if err != nil {
			log.Fatal(err)
		}
	}()
	log.Println("Subscriber started")

	// Start a publisher to the input subject in a goroutine
	log.Println("Starting publisher to input subject...")
	go func() {
		times := 2
		for i := 0; i < times; i++ {
			// Publish the list of items to the input subject
			for _, item := range items {
				itemBytes, err := json.Marshal(item)
				if err != nil {
					log.Fatal(err)
				}
				err = nc.Publish(subjectIn, itemBytes)
				if err != nil {
					log.Fatal(err)
				}
			}
			log.Printf("Published %d items to input subject\n", size)
		}

		// Check how many items were received by the subscriber and compare with input items
		time.Sleep(1 * time.Second)
		log.Printf("Received %d items on output subject\n", len(storage))
		if len(storage) != size {
			log.Fatalf("Expected %d items, got %d\n", size, len(storage))
		}
		for i := 0; i < size; i++ {
			if storage[i].ID != items[i].ID {
				log.Fatalf("Expected %v, got %v\n", items[i], storage[i])
			}
		}
		log.Println("All items received correctly")
	}()
	log.Println("Publisher started")

	log.Println("Waiting for messages...")
	// Keep the connection alive until the program is terminated
	select {}
}

func dataSetup(size int) []*item {
	items := make([]*item, size)
	for i := 0; i < size; i++ {
		items[i] = &item{
			ID:  i,
			Key: "key" + strconv.Itoa(i),
			Val: "val" + strconv.Itoa(i),
		}
	}

	return items
}
