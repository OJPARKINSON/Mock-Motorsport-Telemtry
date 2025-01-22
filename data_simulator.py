import json
import time
import paho.mqtt.client as mqtt

# MQTT Config
BROKER = "localhost"
TOPIC = "f1/telemetry"

def main():
    # Load seed data
    with open("data/telemetry_seed.json", "r") as file:
        telemetry_data = json.load(file)

    # Connect to MQTT Broker
    client = mqtt.Client()
    client.connect(BROKER, 1883, 60)

    # Simulate telemetry data
    for data_point in telemetry_data:
        payload = json.dumps(data_point)
        client.publish(TOPIC, payload)
        print(f"Published: {payload}")
        time.sleep(1)  # Simulate 1-second intervals

if __name__ == "__main__":
    main()