import os
import io
import time
import struct
import random
import threading
import requests
from flask import Flask, render_template_string, request, redirect
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

app = Flask(__name__)

# Global Runtime Status Flags
AUTO_GENERATOR_ACTIVE = False
LATEST_DELIVERY_STATUS = "Awaiting deployment action..."

# 1. Environment Configuration Setup
KAFKA_BOOTSTRAP = os.environ.get("AIVEN_BOOTSTRAP_SERVER")
AIVEN_REGISTRY_URL = os.environ.get("AIVEN_SCHEMA_REGISTRY_URL")
TOPIC_NAME = "machine-events"
SCHEMA_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-schema/versions/latest/schema"

print("🔐 Unpacking secure Aiven SSL network keys...")
with open("ca.pem", "w") as f: f.write(os.environ.get("AIVEN_CA_CERT", ""))
with open("service.cert", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_CERT", ""))
with open("service.key", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_KEY", ""))

print("🔄 Syncing structural telemetry blueprint from Aiven Karapace...")
response = requests.get(SCHEMA_URL, timeout=5.0)
avro_schema = parse_schema(response.json())

# Secure Mutual SSL Producer Mappings
kafka_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SSL",
    "ssl.ca.location": "ca.pem",
    "ssl.certificate.location": "service.cert",
    "ssl.key.location": "service.key"
})

def delivery_report(err, msg):
    global LATEST_DELIVERY_STATUS
    if err is not None:
        LATEST_DELIVERY_STATUS = f"❌ Delivery failed: {err}"
    else:
        LATEST_DELIVERY_STATUS = f"🚀 Streamed! Offset: {msg.offset()} | Part: {msg.partition()} | Time: {time.strftime('%H:%M:%S')}"

def send_to_kafka(mc_name, temp, pressure):
    try:
        payload = {"mc_name": mc_name, "temp": float(temp), "pressure": float(pressure)}
        bytes_io = io.BytesIO()
        schemaless_writer(bytes_io, avro_schema, payload)
        raw_avro_binary = bytes_io.getvalue()

        # Prepend Confluent 5-byte wire header wire chunk mapping
        header = struct.pack(">bI", 0, 1)
        final_payload = header + raw_avro_binary

        kafka_producer.produce(TOPIC_NAME, value=final_payload, callback=delivery_report)
        kafka_producer.flush()
        return True
    except Exception as e:
        global LATEST_DELIVERY_STATUS
        LATEST_DELIVERY_STATUS = f"❌ Serialization Error: {e}"
        return False

def background_random_generator():
    global AUTO_GENERATOR_ACTIVE
    machines = ["Press-01", "Press-02", "Press-03", "Press-04", "Press-05"]
    while True:
        if AUTO_GENERATOR_ACTIVE:
            selected_machine = random.choice(machines)
            temp = round(random.uniform(32.0, 68.0), 1)      
            pressure = round(random.uniform(95.0, 155.0), 1)
            send_to_kafka(selected_machine, temp, pressure)
        time.sleep(1.5) 

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>IIoT Telemetry Injection Control Panel</title>
    <style>
        body { font-family: monospace; background-color: #1e1e24; color: #ffffff; padding: 25px; }
        .box { max-width: 600px; margin: 0 auto; background: #2a2a35; padding: 20px; border-radius: 6px; }
        .status { background: #000; padding: 10px; color: #00ff00; border-radius: 4px; margin: 15px 0; }
        .btn { background: #00cc66; border: none; color: white; padding: 10px 15px; font-weight: bold; cursor: pointer; border-radius: 4px; }
        .btn-stop { background: #ff3333; }
        .form-group { margin-bottom: 12px; }
        label { display: block; margin-bottom: 4px; color: #ffcc00; }
        select, input[type="number"] { width: 98%; padding: 8px; border: 1px solid #444; background: #111; color: #fff; border-radius: 4px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="box">
        <h2>🏭 IIoT Telemetry Injection Engine (5-Machine Control)</h2>
        <div class="status">📬 Latest Node Delivery Status:<br><strong>{{ status }}</strong></div>
        
        <h3>🤖 Mode A: Automated Multi-Asset Generation Loop</h3>
        <form action="/toggle-auto" method="post">
            {% if auto_active %}
                <p>Status: <span style="color:#00ff00; font-weight:bold;">GENERATING 5-MACHINE STREAM (1.5s)</span></p>
                <input type="submit" class="btn btn-stop" value="Stop Auto-Generator">
            {% else %}
                <p>Status: <span style="color:#888;">IDLE</span></p>
                <input type="submit" class="btn" value="Start 5-Machine Stream Loop">
            {% endif %}
        </form>
        <hr style="border: 0; border-top: 1px solid #444; margin: 20px 0;">
        
        <h3>✍️ Mode B: Manual Override Injection (Target Specific Asset)</h3>
        <form action="/manual-send" method="post">
            <div class="form-group">
                <label>Target Machine Asset ID:</label>
                <select name="mc_name">
                    <option value="Press-01">Press-01</option>
                    <option value="Press-02">Press-02</option>
                    <option value="Press-03">Press-03</option>
                    <option value="Press-04" selected>Press-04</option>
                    <option value="Press-05">Press-05</option>
                </select>
            </div>
            <div class="form-group">
                <label>Temperature (°C):</label>
                <input type="number" step="0.1" name="temp" value="42.5" required>
            </div>
            <div class="form-group">
                <label>Pressure (PSI):</label>
                <input type="number" step="0.1" name="pressure" value="115.0" required>
            </div>
            <input type="submit" class="btn" style="background:#0099ff;" value="Fire Targeted Payload Packet">
        </form>
    </div>
</body>
</html>
"""

@app.route('/')
def control_panel():
    return render_template_string(HTML_TEMPLATE, status=LATEST_DELIVERY_STATUS, auto_active=AUTO_GENERATOR_ACTIVE)

# 🔥 FIXED: Swapped 'method' to plural 'methods'
@app.route('/toggle-auto', methods=['POST'])
def toggle_auto():
    global AUTO_GENERATOR_ACTIVE
    AUTO_GENERATOR_ACTIVE = not AUTO_GENERATOR_ACTIVE
    return redirect('/')

# 🔥 FIXED: Swapped 'method' to plural 'methods'
@app.route('/manual-send', methods=['POST'])
def manual_send():
    mc_name = request.form.get("mc_name")
    temp = request.form.get("temp")
    pressure = request.form.get("pressure")
    send_to_kafka(mc_name, temp, pressure)
    return redirect('/')

if __name__ == '__main__':
    threading.Thread(target=background_random_generator, daemon=True).start()
    bind_port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=bind_port)
