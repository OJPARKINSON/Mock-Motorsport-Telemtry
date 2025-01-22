from flask import Flask, render_template
import json
import threading
import paho.mqtt.client as mqtt

app = Flask(__name__)

# Telemetry Storage
telemetry_data = []

# MQTT Config
BROKER = "localhost"
TOPIC = "f1/telemetry"

# MQTT Callback
def on_message(client, userdata, msg):
    global telemetry_data
    data = json.loads(msg.payload.decode())
    telemetry_data.append(data)
    if len(telemetry_data) > 10:  # Keep only the latest 10 entries
        telemetry_data.pop(0)

# MQTT Subscriber Thread
def mqtt_listener():
    client = mqtt.Client()
    client.connect(BROKER, 1883, 60)
    client.subscribe(TOPIC)
    client.on_message = on_message
    client.loop_forever()

# Flask Routes
@app.route("/")
def dashboard():
    return render_template("dashboard.html", telemetry=telemetry_data)

if __name__ == "__main__":
    threading.Thread(target=mqtt_listener).start()
    app.run(debug=True, port=5000)