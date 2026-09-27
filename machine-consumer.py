import os
import io
import time
import json
import hashlib
import requests
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader

# 1. Environment Configuration Extraction
# (Render will supply these parameters dynamically into the container space)
KAFKA_BOOTSTRAP = os.environ.get("AIVEN_BOOTSTRAP_SERVER")
AIVEN_REGISTRY_URL = os.environ.get("AIVEN_SCHEMA_REGISTRY_URL")

TOPIC_NAME = "machine-events"
DLQ_TOPIC_NAME = "machine-events-dlq"
CONSUMER_GROUP = "machine-monitor-group"

# Aiven Karapace Schema Registry endpoint formats
SCHEMA_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-schema/versions/latest/schema"
CONTROL_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-control/versions/latest/schema"

# 2. Writing Mutual SSL Authentication Certs on the Fly
# Aiven requires raw text certs to reside inside local files for the underlying C library (librdkafka)
print("🔐 Instantiating secure Aiven mutual authentication SSL certificates...")
with open("ca.pem", "w") as f: f.write(os.environ.get("AIVEN_CA_CERT", ""))
with open("service.cert", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_CERT", ""))
with open("service.key", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_KEY", ""))

# Global threshold runtime variables
TEMP_MAX = 50.0
PRESSURE_MAX = 150.0
CURRENT_CONFIG_HASH = "" 
last_config_check = 0
CHECK_INTERVAL_SECONDS = 10.0 

def check_for_workflow_updates():
    global TEMP_MAX, PRESSURE_MAX, CURRENT_CONFIG_HASH
    try:
        # Aiven Karapace provides the raw schema layout text under credentials if configured
        res = requests.get(CONTROL_URL, timeout=5.0)
        if res.status_code == 200:
            raw_text = res.text
            
            # Compute distinct thumbprint hash signatures
            incoming_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()
            
            if incoming_hash != CURRENT_CONFIG_HASH:
                CURRENT_CONFIG_HASH = incoming_hash
                profile = json.loads(raw_text)
                
                TEMP_MAX = profile["rules"]["max_safe_temp"]
                PRESSURE_MAX = profile["rules"]["max_safe_pressure"]
                
                print(f"\n📡 [CLOUD RECONCILIATION] New GitOps Rule Profile Swapped via Aiven!")
                print(f"🔑 Active Config Hash thumbprint: {CURRENT_CONFIG_HASH[:8]}")
                print(f"⚙️ Operational Constraints Updated -> Max Temp: {TEMP_MAX}°C | Max Pressure: {PRESSURE_MAX} PSI\n")
    except Exception as e:
        print(f"⚠️ Cloud workflow sync warning: {e}")

# 3. System Initialization Sequence
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

# 4. Strict Secure Aiven Cloud Stream Consumers Setup
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

# Secure Aiven Cloud Dead Letter Queue Producer Link
dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SSL",
    "ssl.ca.location": "ca.pem",
    "ssl.certificate.location": "service.cert",
    "ssl.key.location": "service.key"
})

print(f"🎧 Control engine active. Monitoring Aiven stream on '{TOPIC_NAME}'...")

# 5. Ingestion Processing Loop
try:
    while True:
        current_time = time.time()
        if current_time - last_config_check > CHECK_INTERVAL_SECONDS:
            check_for_workflow_updates()
            last_config_check = current_time

        msg = consumer.poll(0.5) 
        if msg is None: continue
        if msg.error(): print(f"Kafka Cloud Error: {msg.error()}"); break

        raw_bytes = msg.value()
        try:
            # Wire format layout check (Slices off the 5-byte identifier chunk cleanly if present)
            actual_avro_bytes = raw_bytes[5:] if len(raw_bytes) > 5 and raw_bytes[0] == 0 else raw_bytes
            bytes_io = io.BytesIO(actual_avro_bytes)
            record = schemaless_reader(bytes_io, avro_schema)
            
            breaches = []
            if record["temp"] > TEMP_MAX:
                breaches.append(f"Temperature: {record['temp']}°C (Limit: {TEMP_MAX}°C)")
            if record["pressure"] > PRESSURE_MAX:
                breaches.append(f"Pressure: {record['pressure']} PSI (Limit: {PRESSURE_MAX} PSI)")
                
            if breaches:
                print(f"🚨 ALARM [Hash: {CURRENT_CONFIG_HASH[:8]}]: Machine '{record['mc_name']}' Breached Boundaries! -> {' | '.join(breaches)}")
            else:
                print(f"📥 Telemetry Logged [Offset {msg.offset()}][Hash: {CURRENT_CONFIG_HASH[:8]}]: {record['mc_name']} is stable.")

        except Exception as structural_error:
            print(f"⚠️ Caught Structural Poison Pill at Cloud Offset {msg.offset()}: {structural_error}")
            dlq_producer.produce(DLQ_TOPIC_NAME, value=raw_bytes)
            dlq_producer.flush()

except KeyboardInterrupt:
    print("\n🛑 Cloud factory monitoring stopped safely.")
finally:
    consumer.close()
