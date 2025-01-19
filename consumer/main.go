package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "kafka:9092"
	topic       = "telemetry"
)

var db *sql.DB

func initDB() {
	connStr := "postgres://user:password@db:5432/telemetrydb?sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS telemetry (
        id SERIAL PRIMARY KEY,
        speed FLOAT,
        rpm FLOAT,
        engine_temp FLOAT,
        gps_lat FLOAT,
        gps_long FLOAT,
        timestamp TIMESTAMP
    )`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

func saveToDatabase(telemetry map[string]float64) {
	_, err := db.Exec(`
        INSERT INTO telemetry (speed, rpm, engine_temp, gps_lat, gps_long, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6)`,
		telemetry["speed"], telemetry["rpm"], telemetry["engine_temp"], telemetry["gps_lat"], telemetry["gps_long"], time.Now())

	if err != nil {
		log.Println("Failed to save telemetry to database:", err)
	} else {
		log.Println("Telemetry saved to database")
	}
}

func consumeTelemetry() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: "telemetry-consumers",
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(nil)
		if err != nil {
			log.Println("Failed to read message:", err)
			continue
		}

		fmt.Printf("Received message: %s\n", string(message.Value))
		// Simulate telemetry data saving
		telemetry := map[string]float64{
			"speed":       120.0, // Extract data from message.Value
			"rpm":         6000.0,
			"engine_temp": 90.0,
			"gps_lat":     50.0,
			"gps_long":    10.0,
		}
		saveToDatabase(telemetry)
	}
}

func main() {
	initDB()
	log.Println("Starting telemetry consumer...")
	consumeTelemetry()
}
