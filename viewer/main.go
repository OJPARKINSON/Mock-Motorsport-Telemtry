package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "kafka:9092"
	topic       = "telemetry"
	dbConnStr   = "postgres://user:password@db:5432/telemetrydb?sslmode=disable"
)

var db *sql.DB
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func initDB() {
	var err error
	db, err = sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
}

// Historical data endpoint
func getHistoricalData(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT speed, rpm, engine_temp, gps_lat, gps_long, timestamp FROM telemetry ORDER BY timestamp DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "Failed to query data", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var telemetryData []map[string]interface{}
	for rows.Next() {
		var speed, rpm, engineTemp, gpsLat, gpsLong float64
		var timestamp string
		if err := rows.Scan(&speed, &rpm, &engineTemp, &gpsLat, &gpsLong, &timestamp); err != nil {
			http.Error(w, "Failed to scan data", http.StatusInternalServerError)
			return
		}
		telemetryData = append(telemetryData, map[string]interface{}{
			"speed":       speed,
			"rpm":         rpm,
			"engine_temp": engineTemp,
			"gps_lat":     gpsLat,
			"gps_long":    gpsLong,
			"timestamp":   timestamp,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(telemetryData)
}

// WebSocket connection for live telemetry data
func handleLiveTelemetry(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed to upgrade to WebSocket", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: "viewer-consumers",
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(nil)
		if err != nil {
			log.Println("Failed to read message from Kafka:", err)
			break
		}

		// Send message to WebSocket client
		err = conn.WriteMessage(websocket.TextMessage, message.Value)
		if err != nil {
			log.Println("Failed to write message to WebSocket:", err)
			break
		}
	}
}

func main() {
	initDB()
	defer db.Close()

	http.HandleFunc("/historical", getHistoricalData)
	http.HandleFunc("/live", handleLiveTelemetry)

	fmt.Println("Viewer service running on port 8082...")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
