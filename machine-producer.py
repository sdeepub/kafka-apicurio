import os
import io
import struct
import requests
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

# 1. Environment Configuration Setup
KAFKA_BOOTSTRAP = os.environ.get("AIVEN_BOOTSTRAP_SERVER")
AIVEN_REGISTRY_URL = os.environ.get("AIVEN_SCHEMA_REGISTRY_URL")

TOPIC_NAME = "machine-events"
SCHEMA_URL = f"{AIVEN_REGISTRY_URL}/subjects/machine-schema/versions/latest/schema"

# 2. Extract Security Certificates on the fly
# Aiven's mutual authentication setup requires these files to reside locally on disk
print("🔐 Unpacking secure Aiven SSL network keys...")
with open("ca.pem", "w") as f: f.write(os.environ.get("AIVEN_CA_CERT", ""))
with open("service.cert", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_CERT", ""))
with open("service.key", "w") as f: f.write(os.environ.get("AIVEN_SERVICE_KEY", ""))

print("🔄 Syncing structural telemetry blueprint from Aiven Karapace Registry...")
try:
    response = requests.get(SCHEMA_URL, timeout=5.0)
    if response.status_code != 200:
        print(f"❌ Registry lookup error. HTTP Status: {response.status_code}")
        exit(1)
    avro_schema = parse_schema(response.json())
    print("✅ Schema compiled successfully!")
except Exception as e:
    print(f"❌ Failed to reach registry: {e}")
    exit(1)

# 3. Secure Mutual SSL Producer Configuration Mappings
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SSL",
    "ssl.ca.location": "ca.pem",
    "ssl.certificate.location": "service.cert",
    "ssl.key.location": "service.key"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Cloud message delivery failed: {err}")
    else:
        print(f"🚀 Telemetry successfully streamed to Aiven! [Topic: {msg.topic()} | Partition: {msg.partition()}]")

# Simulated High-Precision Industrial Machine Row Payload
payload = {
    "mc_name": "Press-Line-04",
    "temp": 54.8,       
    "pressure": 122.4
}

try:
    # 4. Serialize data natively using schemaless_writer
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, avro_schema, payload)
    raw_avro_binary = bytes_io.getvalue()

    # Prepend the 5-byte identifier header mapping wire format 
    # (Magic Byte 0 + Schema ID 1 placeholder)
    SCHEMA_ID = 1
    header = struct.pack(">bI", 0, SCHEMA_ID)
    final_payload = header + raw_avro_binary

    # 5. Stream the binary block to Aiven Cloud Broker
    producer.produce(TOPIC_NAME, value=final_payload, callback=delivery_report)
    producer.flush()

except Exception as e:
    print(f"❌ Structural Validation or Serialization Error: {e}")
