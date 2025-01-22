import paho.mqtt.client as mqtt

# Callback when the client receives a connection acknowledgment
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Broker started successfully. Listening for connections...")
    else:
        print(f"Connection failed with return code {rc}")

# Main function to start the MQTT broker
def main():
    client = mqtt.Client()
    client.on_connect = on_connect

    # Binding to the localhost
    try:
        client.connect("localhost", 1883, 60)  # Adjust the host/port if needed
        client.loop_forever()
    except Exception as e:
        print(f"Error starting the broker: {e}")

if __name__ == "__main__":
    main()
