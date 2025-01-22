import json
import paho.mqtt.client as mqtt

# MQTT Config
BROKER = "localhost"
TOPIC = "f1/telemetry"

def process_data(data):
    # Simulate processing: Print out data
    print(f"Processing Data: {data}")
    # Example: Anomaly detection
    if data["engine_temp"] > 110:
        print(f"ALERT: High engine temperature for car {data['car_id']}!")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    process_data(data)

def main():
    client = mqtt.Client()
    client.connect(BROKER, 1883, 60)
    client.subscribe(TOPIC)
    client.on_message = on_message
    print("Data Processor: Listening for telemetry...")
    client.loop_forever()

if __name__ == "__main__":
    main()