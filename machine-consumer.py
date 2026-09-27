import io
import time
import requests
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader

# 1. Configuration Setup
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "machine-events"
DLQ_TOPIC_NAME = "machine-events-dlq"
CONSUMER_GROUP = "machine-monitor-group"

# Hardcoded version tracker matching your current active Apicurio artifact phase
MANUAL_CONTROL_VERSION = 3  # 👈 Toggle this to 1, 2, or 3 as you evolve rules!

APICURIO_SCHEMA_URL = "http://localhost:8080/apis/registry/v3/groups/default/artifacts/machine-schema/versions/1/content"
# Direct resource path targeting the specific version content you want to lock down
APICURIO_CONTROL_URL = f"http://localhost:8080/apis/registry/v3/groups/default/artifacts/machine-control/versions/{MANUAL_CONTROL_VERSION}/content"

# Global threshold variables
TEMP_MAX = 50.0
PRESSURE_MAX = 150.0
last_config_check = 0
CHECK_INTERVAL_SECONDS = 10.0 

def fetch_latest_control_rules():
    global TEMP_MAX, PRESSURE_MAX
    try:
        # Request the explicit version rules content payload
        control_res = requests.get(APICURIO_CONTROL_URL, timeout=2.0)
        
        if control_res.status_code == 200:
            profile = control_res.json()
            TEMP_MAX = profile["rules"]["max_safe_temp"]
            PRESSURE_MAX = profile["rules"]["max_safe_pressure"]
    except Exception as e:
        print(f"⚠️ Warning: Could not refresh control rules from Apicurio: {e}")

# 2. Sequential System Initialization Flow
print("🔄 Booting system. Pulling initial data layout schema...")
schema_res = requests.get(APICURIO_SCHEMA_URL)
if schema_res.status_code != 200:
    print("❌ Critical structural schema pull failure.")
    exit(1)

avro_schema = parse_schema(schema_res.json())

# Fetch the static schema version content properties
fetch_latest_control_rules() 

# 3. Kafka Stream Client Configuration
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest"
})
consumer.subscribe([TOPIC_NAME])
dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Confirmation output explicitly uses your local static version parameter
print(f"🎧 Control engine active. Monitoring '{TOPIC_NAME}' stream... [Current Config Rule Profile: v.{MANUAL_CONTROL_VERSION}]")

# 4. Stream Ingestion Processing Routing Engine
try:
    while True:
        # Check Apicurio for active configuration rules adaptations periodically
        current_time = time.time()
        if current_time - last_config_check > CHECK_INTERVAL_SECONDS:
            fetch_latest_control_rules()
            last_config_check = current_time

        msg = consumer.poll(0.5) 
        if msg is None: continue
        if msg.error(): print(f"Kafka Error: {msg.error()}"); break

        raw_bytes = msg.value()
        try:
            # Wire format extraction check (Strip the 5-byte header chunk cleanly if spoofed)
            actual_avro_bytes = raw_bytes[5:] if len(raw_bytes) > 5 and raw_bytes[0] == 0 else raw_bytes
            bytes_io = io.BytesIO(actual_avro_bytes)
            record = schemaless_reader(bytes_io, avro_schema)
            
            # Evaluate telemetry metrics records against current control profile boundaries
            breaches = []
            if record["temp"] > TEMP_MAX:
                breaches.append(f"Temperature: {record['temp']}°C (Limit: {TEMP_MAX}°C)")
            if record["pressure"] > PRESSURE_MAX:
                breaches.append(f"Pressure: {record['pressure']} PSI (Limit: {PRESSURE_MAX} PSI)")
                
            if breaches:
                print(f"🚨 ALARM [Config v.{MANUAL_CONTROL_VERSION}]: Machine '{record['mc_name']}' Breached Safe Operations! -> {' | '.join(breaches)}")
            else:
                print(f"📥 Telemetry Logged [Offset {msg.offset()}][Config v.{MANUAL_CONTROL_VERSION}]: {record['mc_name']} is normal ({record['temp']}°C, {record['pressure']} PSI).")

        except Exception as structural_error:
            # Drop malformed packets directly out to the Dead Letter Queue highway 
            print(f"⚠️ Caught Structural Poison Pill at Offset {msg.offset()}: {structural_error}")
            dlq_producer.produce(DLQ_TOPIC_NAME, value=raw_bytes)
            dlq_producer.flush()

except KeyboardInterrupt:
    print("\n🛑 Factory monitoring stopped safely.")
finally:
    consumer.close()
