package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func generateTelemetry() map[string]float64 {
	return map[string]float64{
		"speed":       float64(rand.Intn(200)),
		"rpm":         float64(rand.Intn(9000)),
		"engine_temp": float64(rand.Intn(120)),
		"gps_lat":     rand.Float64() * 180,
		"gps_long":    rand.Float64() * 180,
	}
}

func telemetryHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		telemetry := generateTelemetry()
		err := conn.WriteJSON(telemetry)
		if err != nil {
			fmt.Println("Write error:", err)
			return
		}
	}
}

func main() {
	http.HandleFunc("/telemetry", telemetryHandler)
	fmt.Println("Starting telemetry producer on :8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Server error:", err)
	}
}
