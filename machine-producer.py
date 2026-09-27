import io
import os
import time
import json
import hashlib
import threading
import requests
from flask import Flask, render_template_string
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader

# ==============================================================================
# 1. INTERACTIVE WEB PORTAL CONFIGURATION
# ==============================================================================
app = Flask(__name__)

DASHBOARD_LOGS = []
TEMP_MAX = 40.0       
PRESSURE_MAX = 120.0
CURRENT_CONFIG_HASH = "unknown"

def log_to_dashboard(msg_text):
    global DASHBOARD_LOGS
    DASHBOARD_LOGS.append(msg_text) # Kept raw text layout matching console style
    if len(DASHBOARD_LOGS) > 25:  
        DASHBOARD_LOGS.pop(0)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Cloud IIoT Control Engine Dashboard</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { font-family: monospace; background-color: #1a1a1a; color: #00ff00; padding: 25px; font-size: 14px; }
        .wrapper { max-width: 1050px; margin: 0 auto; }
        .card { border: 1px solid #00ff00; padding: 18px; margin-bottom: 20px; border-radius: 4px; }
        .log-line { margin: 8px 0; color: #ffffff; line-height: 1.4; }
        .alarm { color: #ff3333; font-weight: bold; }
        .success { color: #33ff99; }
    </style>
</head>
<body>
    <div class="wrapper">
        <h2>🏭 Factory Telemetry Control Center (Live Cloud Ingestion Engine)</h2>
        <div class="card">
            <h3>📡 Active Boundaries (Pulled dynamically via Aiven Karapace)</h3>
            <p>ℹ️ Current Configuration Fingerprint: <strong>{{ active_hash }}</strong></p>
            <p>🔥 Max Safe Temperature Threshold: <strong style="color:#ffcc00;">{{ max_temp }}°C</strong></p>
            <p>💨 Max Safe Pressure Threshold: <strong style="color:#00ccff;">{{ max_press }} PSI</strong></p>
        </div>
        <div class="card">
            <h3>📊 Live Pipeline Stream Analytics (Descriptive Formatting)</h3>
            <div style="background-color: #000000; padding: 20px; border-radius: 4px; max-height: 450px; overflow-y: auto;">
                {% for log in logs %}
                    <div class="log-line {% if '🚨' in log %}alarm{% else %}success{% endif %}">{{ log }}</div>
                {% endfor %}
            </div>
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def live_dashboard():
    return render_template_string(
        HTML_TEMPLATE,
        logs=reversed(DASHBOARD_LOGS),
        max_temp=TEMP_MAX,
        max_press=PRESSURE_MAX,
        active_hash=CURRENT_CONFIG_HASH[:8]
    )

def start_render_http_listener():
    bind_port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=bind_port, debug=False, use_reloader=False)

# ==============================================================================
# 2. CORE STREAM CONSUMER LOGIC (Smart Header Slicing)
# ==============================================================================
KAFKA_BOOTSTRAP = os.environ.get("AIVEN_BOOTSTRAP_SERVER")
AIVEN_REGISTRY_URL = os.environ.get("AIVEN_SCHEMA_REGISTRY_URL")

TOPIC_NAME = "machine-events"
DLQ_TOPIC_NAME = "machine-events-dlq"
CONSUMER_GROUP = "machine-monitor-group"

SCHEMA_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-schema/versions/latest/schema"
CONTROL_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-control/versions/latest/schema"

print("🔐 Instantiating secure Aiven mutual authentication SSL certificates...")
with open("ca.pem", "w") as f: f.write(os.environ.get("AIVEN_CA_CERT", ""))
with open("service.cert", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_CERT", ""))
with open("service.key", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_KEY", ""))

last_config_check = 0
CHECK_INTERVAL_SECONDS = 10.0 

def check_for_workflow_updates():
    global TEMP_MAX, PRESSURE_MAX, CURRENT_CONFIG_HASH
    try:
        res = requests.get(CONTROL_URL, timeout=5.0)
        if res.status_code == 200:
            raw_text = res.text
            incoming_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()
            
            if incoming_hash != CURRENT_CONFIG_HASH:
                CURRENT_CONFIG_HASH = incoming_hash
                profile = json.loads(raw_text)
                
                TEMP_MAX = profile["rules"]["max_safe_temp"]
                PRESSURE_MAX = profile["rules"]["max_safe_pressure"]
                
                print(f"📡 [CONFIG SYNC] Hash: {CURRENT_CONFIG_HASH[:8]} | Limits -> Temp: {TEMP_MAX}°C, Press: {PRESSURE_MAX} PSI")
    except Exception as e:
        print(f"⚠️ Cloud workflow sync warning: {e}")

print("🔄 Connecting to Aiven Karapace to pull data layout schema...")
schema_res = requests.get(SCHEMA_URL, timeout=5.0)
if schema_res.status_code != 200:
    print(f"❌ Critical structural schema pull failure. HTTP {schema_res.status_code}")
    exit(1)
avro_schema = parse_schema(schema_res.json())
print("✅ Structural schema contract compiled successfully!")

check_for_workflow_updates()

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "latest", # Skip any old malformed backlog data strings
    "security.protocol": "SSL",
    "ssl.ca.location": "ca.pem",
    "ssl.certificate.location": "service.cert",
    "ssl.key.location": "service.key"
})
consumer.subscribe([TOPIC_NAME])

dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SSL",
    "ssl.ca.location": "ca.pem",
    "ssl.certificate.location": "service.cert",
    "ssl.key.location": "service.key"
})

# Launch parallel web visualization engine
threading.Thread(target=start_render_http_listener, daemon=True).start()
print(f"🎧 Control engine active. Monitoring '{TOPIC_NAME}' stream...")

try:
    while True:
        current_time = time.time()
        if current_time - last_config_check > CHECK_INTERVAL_SECONDS:
            check_for_workflow_updates()
            last_config_check = current_time

        msg = consumer.poll(0.5) 
        if msg is None: continue
        if msg.error(): print(f"Kafka Error: {msg.error()}"); break

        raw_bytes = msg.value()
        try:
            # 🔍 SMART WIRE-FORMAT RESOLVER:
            # If the byte array starts with magic byte \x00, check if we need to slice it safely.
            if len(raw_bytes) > 5 and raw_bytes[0] == 0:
                # Test parsing the raw bytes starting at offset 5 vs offset 0 dynamically
                try:
                    bytes_io = io.BytesIO(raw_bytes[5:])
                    record = schemaless_reader(bytes_io, avro_schema)
                    # Verify fields parsed into standard real-world ranges instead of corrupted text
                    if not record.get("mc_name") or record.get("temp", 0) > 1000:
                        raise ValueError("Slicing caused data misalignment")
                except Exception:
                    # Fallback to absolute raw array if slicing shifts data improperly
                    bytes_io = io.BytesIO(raw_bytes)
                    record = schemaless_reader(bytes_io, avro_schema)
            else:
                bytes_io = io.BytesIO(raw_bytes)
                record = schemaless_reader(bytes_io, avro_schema)

            # Extract fields safely 
            mc_name = record.get("mc_name", "Unknown-Machine")
            temp = round(record.get("temp", 0.0), 1)
            pressure = round(record.get("pressure", 0.0), 1)
            config_ver = CURRENT_CONFIG_HASH[:8]

            # Evaluate thresholds boundaries
            is_temp_breached = temp > TEMP_MAX
            is_press_breached = pressure > PRESSURE_MAX

            # ==============================================================================
            # 3. HIGH-PRECISION DESCRIPTIVE LOG GENERATOR
            # ==============================================================================
            timestamp = time.strftime('%H:%M:%S')
            
            if is_temp_breached or is_press_breached:
                breaches = []
                if is_temp_breached:
                    breaches.append(f"Temperature: {temp}°C (Limit: {TEMP_MAX}°C)")
                if is_press_breached:
                    breaches.append(f"Pressure: {pressure} PSI (Limit: {PRESSURE_MAX} PSI)")
                
                # Dynamic Descriptive Alarm Layout String
                log_output = f"[{timestamp}] 🚨 ALARM [Config v.{config_ver}]: Machine '{mc_name}' Breached Safe Operations! -> {' | '.join(breaches)}"
            else:
                # Dynamic Clean Ingestion Layout String
                log_output = f"[{timestamp}] 📥 Telemetry Logged [Offset {msg.offset()}][Config v.{config_ver}]: {mc_name} is normal ({temp}°C, {pressure} PSI)."

            print(log_output)
            log_to_dashboard(log_output)

        except Exception as structural_error:
            timestamp = time.strftime('%H:%M:%S')
            poison_msg = f"[{timestamp}] ⚠️ Caught Poison Pill at Offset {msg.offset()}: {structural_error} -> Routing to DLQ."
            print(poison_msg)
            log_to_dashboard(poison_msg)
            dlq_producer.produce(DLQ_TOPIC_NAME, value=raw_bytes)
            dlq_producer.flush()

except KeyboardInterrupt:
    print("\n🛑 Factory monitoring stopped safely.")
finally:
    consumer.close()
