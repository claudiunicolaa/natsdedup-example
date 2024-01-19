# natsdedup example project

This project is an example application that uses the `natsdedup` Go package from [claudiunicolaa](https://github.com/claudiunicolaa/natsdedup).

This example Go app demonstrates NATS server usage with deduplication. It connects to a NATS server, initializes a deduplicator for message processing, and creates a list of data items. It then publishes these items to a NATS subject, while a separate goroutine subscribes to another subject to receive and store these items. In the end, ensures items are not duplicated during transmission, checking that all sent items are correctly received without loss or duplication.

## How to run

Prerequisites:
- docker
- docker-compose
- go

Start a nats server
```bash
docker-compose up -d
```

Start example app
```bash
go run main.go
```
