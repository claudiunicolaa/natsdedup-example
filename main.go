package main

import (
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
)

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

	// Create a list of items to deduplicate
	size := 100
	items := make([]string, size)
	for i := 0; i < size; i++ {
		items[i] = "item" + strconv.Itoa(i)
	}

	// Start a subscriber to the output subject in a goroutine
	var storage []string
	go func() {
		log.Println("Starting subscriber to output subject...")
		_, err := nc.Subscribe(subjectOut, func(m *nats.Msg) {
			storage = append(storage, string(m.Data))
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Start a publisher to the input subject in a goroutine
	go func() {
		log.Println("Starting publisher to input subject...")
		times := 2
		for i := 0; i < times; i++ {
			// Publish the list of items to the input subject
			for _, item := range items {
				err := nc.Publish(subjectIn, []byte(item))
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
			if storage[i] != items[i] {
				log.Fatalf("Expected %s, got %s\n", items[i], storage[i])
			}
		}
	}()

	// Keep the connection alive until the program is terminated
	select {}
}
