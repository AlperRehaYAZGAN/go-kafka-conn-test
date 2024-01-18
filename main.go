package main

import (
	"context"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	// kafka
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/fatih/color"
)

// e.g. consumer: KAFKA_URL="kafka://username:password@host:port/partition" SASL_ENABLED=true KAFKA_CG="consumer-group" KAFKA_TOPIC="topic" ROLE="consumer" go run main.go
// e.g. producer: KAFKA_URL="kafka://username:password@host:port" SASL_ENABLED=true KAFKA_TOPIC="topic" ROLE="producer" go run main.go

// main function is if ROLE=="consumer" then start consumer, else start producer. and send "message: i" to topic.
func main() {
	// get role from env.
	role := os.Getenv("ROLE")

	// if no flag set then show e.g. usage.
	if role == "" {
		color.Red("Usage: ROLE=consumer|producer go run main.go")
		// example usage of consumer
		// e.g. consumer: KAFKA_URL="kafka://username:password@host:port/partition" SASL_ENABLED=true KAFKA_CG="consumer-group" KAFKA_TOPIC="topic" ROLE="consumer" go run main.go
		color.Green("e.g. consumer: KAFKA_URL=\"kafka://username:password@host:port/partition\" SASL_ENABLED=true KAFKA_CG=\"consumer-group\" KAFKA_TOPIC=\"topic\" ROLE=\"consumer\" go run main.go")
		// example usage of producer
		// e.g. producer: KAFKA_URL="kafka://username:password@host:port" SASL_ENABLED=true KAFKA_TOPIC="topic" ROLE="producer" go run main.go
		color.Green("e.g. producer: KAFKA_URL=\"kafka://username:password@host:port\" SASL_ENABLED=true KAFKA_TOPIC=\"topic\" ROLE=\"producer\" go run main.go")

		color.Red("Exiting...")
		return
	}

	// if role is consumer then start consumer.
	if role == "consumer" {
		// create kafka reader (consumer)
		r := NewKafkaConsumerConnection()

		// read messages from kafka
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Println("app.kafka.consumer.read: error while reading message " + err.Error())
				continue
			}

			// log message
			log.Println("app.kafka.consumer.read: message received: " + string(m.Value))
		}
	} else {
		// create kafka writer (producer)
		w := NewKafkaProducerConnection()

		// random str for message
		rand.Seed(time.Now().UnixNano())

		// send messages to kafka
		for i := 0; i < 10; i++ {
			suffix := rand.Intn(1000)

			err := w.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(strconv.Itoa(i)),
					Value: []byte("the message " + strconv.Itoa(i) + " text" + strconv.Itoa(suffix)),
				},
			)
			if err != nil {
				log.Println("app.kafka.producer.write: error while writing message " + err.Error())
				continue
			}

			// log message
			log.Println("app.kafka.producer.write: message sent: " + strconv.Itoa(i))

			time.Sleep(3 * time.Second)
		}
	}

}

func NewKafkaConsumerConnection() *kafka.Reader {
	connStr := os.Getenv("KAFKA_URL")
	cg := os.Getenv("KAFKA_CG")
	topic := os.Getenv("KAFKA_TOPIC")
	// Parse the URL
	parsed, err := url.Parse(connStr)
	if err != nil {
		panic(err)
	}

	// Extract components from the parsed URL
	username := parsed.User.Username()
	password, _ := parsed.User.Password()
	host := parsed.Hostname()
	port := parsed.Port()
	partitionStr := strings.TrimPrefix(parsed.Path, "/")

	// partition is digit string. cast to int.
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		panic(err)
	}

	// create kafka dialer
	dialer := &kafka.Dialer{
		Timeout:   time.Minute,
		DualStack: true,
		// SASLMechanism: mechanism,
	}

	// try to get if set SASL_ENABLED=true from env
	saslEnabled := os.Getenv("SASL_ENABLED")

	// if SASL_ENABLED is true then enable SASL.
	if saslEnabled == "true" || saslEnabled == "TRUE" {
		// scram.Mechanism
		mechanism, err := scram.Mechanism(scram.SHA512, username, password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	// create reader (consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{
			host + ":" + port,
		},
		// kafka generic opts.
		GroupID:   cg,
		Topic:     topic,
		Dialer:    dialer,
		Partition: partition,
		// no wait for message bucket to be full.
		MinBytes: 1,   // 1 byte
		MaxBytes: 1e6, // 1MB
	})

	// log success
	log.Println("app.kafka.consumer.init: kafka consumer connected to topic " + topic + " connection initialized successfully.")

	return r
}

func NewKafkaProducerConnection() *kafka.Writer {
	connStr := os.Getenv("KAFKA_URL")
	defaultTopic := os.Getenv("KAFKA_TOPIC")

	// Parse the URL
	parsed, err := url.Parse(connStr)
	if err != nil {
		panic(err)
	}

	// Extract components from the parsed URL
	username := parsed.User.Username()
	password, _ := parsed.User.Password()
	host := parsed.Hostname()
	port := parsed.Port()

	// create kafka dialer
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		// SASLMechanism: mechanism,
	}

	// try to get if set SASL_ENABLED=true from env
	saslEnabled := os.Getenv("SASL_ENABLED")

	// if SASL_ENABLED is true then enable SASL.
	if saslEnabled == "true" || saslEnabled == "TRUE" {
		// scram.Mechanism
		mechanism, err := scram.Mechanism(scram.SHA512, username, password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	// create producer writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{
			host + ":" + port,
		},
		Topic:    defaultTopic,
		Balancer: &kafka.LeastBytes{},
		Dialer:   dialer,
		Async:    false,
	})

	// log success
	log.Println("app.kafka.producer.init: kafka producer with default topic " + defaultTopic + " connection initialized successfully.")

	return w
}
