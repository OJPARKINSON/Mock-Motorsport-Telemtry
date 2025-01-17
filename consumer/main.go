// consumer.go (for telemetry consumer)
package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

type TelemetryData struct {
	Speed      float64   `json:"speed"`
	RPM        float64   `json:"rpm"`
	EngineTemp float64   `json:"engine_temp"`
	GPSLat     float64   `json:"gps_lat"`
	GPSLong    float64   `json:"gps_long"`
	Timestamp  time.Time `json:"timestamp"`
}

func saveToDatabase(telemetry TelemetryData) {
	// Add logic to save telemetry data to a database (e.g., PostgreSQL)
	fmt.Printf("Saving to DB: %+v\n", telemetry)
}

func broadcastEvent(telemetry TelemetryData) {
	// Broadcast telemetry data to subscribers
}

func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	for {
		var telemetry TelemetryData
		err := conn.ReadJSON(&telemetry)
		if err != nil {
			fmt.Println("Read error:", err)
			break
		}
		telemetry.Timestamp = time.Now()
		saveToDatabase(telemetry)
		broadcastEvent(telemetry)
	}
}

func main() {
	http.HandleFunc("/consume", handleTelemetry)
	log.Println("Telemetry consumer running on :8081")
	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
