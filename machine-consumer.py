import os
import io
import time
import json
import hashlib
import threading
import requests
from flask import Flask, render_template_string
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader

# ==============================================================================
# 1. FLASK WEB SERVER CONFIGURATION LAYER (Bypasses Render Port Scanning)
# ==============================================================================
app = Flask(__name__)

# Global log array to render your streaming metrics visually on your public web URL
DASHBOARD_LOGS = []
TEMP_MAX = 40.0       # Fallback initial tracking limits
PRESSURE_MAX = 120.0
CURRENT_CONFIG_HASH = "unknown"

def log_to_dashboard(msg_text):
    global DASHBOARD_LOGS
    timestamp = time.strftime('%H:%M:%S')
    DASHBOARD_LOGS.append(f"[{timestamp}] {msg_text}")
    if len(DASHBOARD_LOGS) > 20:  # Restrict to last 20 events to save memory
        DASHBOARD_LOGS.pop(0)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Cloud IIoT Control Engine Dashboard</title>
    <meta http-equiv="refresh" content="3"> <!-- Auto-refresh page every 3 seconds -->
    <style>
        body { font-family: monospace; background-color: #1a1a1a; color: #00ff00; padding: 25px; }
        .wrapper { max-width: 900px; margin: 0 auto; }
        .card { border: 1px solid #00ff00; padding: 18px; margin-bottom: 20px; border-radius: 4px; }
        .log-line { margin: 6px 0; color: #ffffff; }
        .alarm { color: #ff3333; font-weight: bold; animation: blink 1.5s infinite; }
        @keyframes blink { 50% { opacity: 0.5; } }
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
            <h3>📊 Live Pipeline Stream Analytics (Auto-Refreshes Continuous Loops)</h3>
            <div style="background-color: #000000; padding: 15px; border-radius: 4px; max-height: 400px; overflow-y: auto;">
                {% for log in logs %}
                    <div class="log-line {% if '🚨' in log %}alarm{% endif %}">{{ log }}</div>
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
    # Render overrides the system port using the PORT environment property
    bind_port = int(os.environ.get("PORT", 10000))
    log_to_dashboard(f"🌐 Activating Flask web infrastructure server on port {bind_port}...")
    app.run(host='0.0.0.0', port=bind_port, debug=False, use_reloader=False)

# ==============================================================================
# 2. STREAM PROCESSING PIPELINE LOGIC (Core Consumer Runtime)
# ==============================================================================
KAFKA_BOOTSTRAP = os.environ.get("AIVEN_BOOTSTRAP_SERVER")
AIVEN_REGISTRY_URL = os.environ.get("AIVEN_SCHEMA_REGISTRY_URL")

TOPIC_NAME = "machine-events"
DLQ_TOPIC_NAME = "machine-events-dlq"
CONSUMER_GROUP = "machine-monitor-group"

SCHEMA_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-schema/versions/latest/schema"
CONTROL_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-control/versions/latest/schema"

# Write out Aiven Mutual TLS credentials to temporary disk space
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
                
                update_msg = f"📡 [CONFIG UPDATE] GitOps Sync! Hash: {CURRENT_CONFIG_HASH[:8]} | Limits -> Temp: {TEMP_MAX}°C, Press: {PRESSURE_MAX} PSI"
                print(update_msg)
                log_to_dashboard(update_msg)
    except Exception as e:
        print(f"⚠️ Cloud workflow sync warning: {e}")

# Load layout blueprint schema prior to consumer loop initialization
print("🔄 Connecting to Aiven Karapace to pull data layout schema...")
try:
    schema_res = requests.get(SCHEMA_URL, timeout=5.0)
    if schema_res.status_code != 200:
        print(f"❌ Critical structural schema pull failure. HTTP {schema_res.status_code}")
        exit(1)
    avro_schema = parse_schema(schema_res.json())
    print("✅ Structural schema contract compiled successfully!")
except Exception as e:
    print(f"❌ Failed to parse schema configuration from registry: {e}")
    exit(1)

check_for_workflow_updates()

# Initialize stream consumers with Mutual SSL encryption options
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
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

# ------------------------------------------------------------------------------
# 3. BACKGROUND THREAD ORCHESTRATION & RUNTIME LOOP
# ------------------------------------------------------------------------------
# Boot Flask web framework loop inside an asynchronous detached parallel execution thread
web_thread = threading.Thread(target=start_render_http_listener, daemon=True)
web_thread.start()

print(f"🎧 Control engine active. Monitoring Aiven stream on '{TOPIC_NAME}'...")
log_to_dashboard(f"🎧 Ingestion engine spun up. Awaiting live data records...")

try:
    while True:
        # Periodic evaluation loop of rule configurations from Aiven Karapace Registry
        current_time = time.time()
        if current_time - last_config_check > CHECK_INTERVAL_SECONDS:
            check_for_workflow_updates()
            last_config_check = current_time

        msg = consumer.poll(0.5) 
        if msg is None: continue
        if msg.error(): 
            err_msg = f"Kafka Cloud Error: {msg.error()}"
            print(err_msg)
            log_to_dashboard(err_msg)
            break

        raw_bytes = msg.value()
        try:
            actual_avro_bytes = raw_bytes[5:] if len(raw_bytes) > 5 and raw_bytes == 0 else raw_bytes
            bytes_io = io.BytesIO(actual_avro_bytes)
            record = schemaless_reader(bytes_io, avro_schema)
            
            breaches = []
            if record["temp"] > TEMP_MAX:
                breaches.append(f"Temp: {record['temp']}°C (Limit: {TEMP_MAX}°C)")
            if record["pressure"] > PRESSURE_MAX:
                breaches.append(f"Pressure: {record['pressure']} PSI (Limit: {PRESSURE_MAX} PSI)")
                
            if breaches:
                alarm_msg = f"🚨 ALARM: Machine '{record['mc_name']}' Breached Boundaries! -> {' | '.join(breaches)}"
                print(alarm_msg)
                log_to_dashboard(alarm_msg)
            else:
                success_msg = f"📥 Ingested Record [Offset {msg.offset()}]: {record['mc_name']} is stable ({record['temp']}°C, {record['pressure']} PSI)."
                print(success_msg)
                log_to_dashboard(success_msg)

        except Exception as structural_error:
            poison_msg = f"⚠️ Caught Poison Pill at Offset {msg.offset()}: {structural_error} -> Rerouting to DLQ."
            print(poison_msg)
            log_to_dashboard(poison_msg)
            dlq_producer.produce(DLQ_TOPIC_NAME, value=raw_bytes)
            dlq_producer.flush()

except KeyboardInterrupt:
    print("\n🛑 Cloud factory monitoring stopped safely.")
finally:
    consumer.close()
