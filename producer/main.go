package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "kafka:9092"
	topic       = "telemetry"
)

func generateTelemetry() map[string]float64 {
	return map[string]float64{
		"speed":       float64(rand.Intn(200)),
		"rpm":         float64(rand.Intn(9000)),
		"engine_temp": float64(rand.Intn(120)),
		"gps_lat":     rand.Float64() * 180,
		"gps_long":    rand.Float64() * 180,
	}
}

func main() {
	writer := kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("Starting telemetry producer...")
	for range ticker.C {
		telemetry := generateTelemetry()
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", rand.Intn(1000))),
			Value: []byte(fmt.Sprintf("%v", telemetry)),
		}

		err := writer.WriteMessages(nil, message)
		if err != nil {
			log.Println("Failed to write message:", err)
		} else {
			log.Println("Produced message:", message.Value)
		}
	}
}
