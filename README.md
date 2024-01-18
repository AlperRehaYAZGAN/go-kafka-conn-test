# Kafka Consumer and Producer Testing

This repository contains a Go application that tests Kafka consumer and producer functionalities using the `github.com/segmentio/kafka-go` package.

## Features

- Kafka Producer: Sends messages to a Kafka topic.
- Kafka Consumer: Reads messages from a Kafka topic.

## Example Usage

First, ensure you have a running Kafka instance that the application can connect to.

Then, you can run the application with the following command:

- Example usage of consumer:
```sh
# template
KAFKA_URL="kafka://username:password@host:port/partition" SASL_ENABLED=true KAFKA_CG="consumer-group" KAFKA_TOPIC="topic" ROLE="consumer" go run main.go
# example
KAFKA_URL="kafka://app:apppwd@localhost:9092/0" SASL_ENABLED=false KAFKA_CG="mail-sender" KAFKA_TOPIC="send-mail" ROLE="consumer" go run main.go
```

- Example usage of producer:
```sh
# template
KAFKA_URL="kafka://username:password@host:port/partition" SASL_ENABLED=true KAFKA_TOPIC="topic" ROLE="producer" go run main.go
# example
KAFKA_URL="kafka://app:apppwd@localhost:9092/0" SASL_ENABLED=false KAFKA_TOPIC="send-mail" ROLE="producer" go run main.go
```

You can run seperate terminals for consumer and producer.

## Environment Variables

| Variable | Description | Default Value |
| -------- | ----------- | ------------- |
| KAFKA_URL | Kafka URL | kafka://username:password@localhost:9092/partition-as-integer-like-0 |
| SASL_ENABLED | SASL authentication enabled (true/false) | false |
| KAFKA_CG | Kafka consumer group (for consumer only) | empty |
| KAFKA_TOPIC | Kafka topic for listen or produce | empty |
| ROLE | Role of the application (consumer/producer) | consumer |

