import io
import json
import struct
import requests
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

# 1. Configuration Setup
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "user-events"
APICURIO_URL = "http://localhost:8080/apis/registry/v3/groups/default/artifacts/481ebcd6-801a-4c18-8915-8ededc077d41/versions/1/content"

print("🔄 Fetching native schema from Apicurio...")
# 2. Grab the raw Avro JSON directly from your custom Apicurio Artifact ID
response = requests.get(APICURIO_URL)
if response.status_code != 200:
    print(f"❌ Failed to fetch schema. Status code: {response.status_code}")
    exit(1)

# Parse the schema using fastavro so it understands the union rules
avro_schema = parse_schema(response.json())
print("✅ Schema loaded successfully!")

# 3. Initialize the pure Kafka Producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"🚀 Message successfully streamed to topic '{msg.topic()}' [Partition: {msg.partition()}]")

# 4. Your pure, standard JSON message payload
# Notice we use plain JSON here because fastavro gracefully manages the standard format!
payload = {
    "id": "usr_2007",
    "name": "James Redis",
    "email": "jr@example.com"  # No weird Confluent {"string": ...} nesting required!
}

try:
    # 5. Serialize data natively using schemaless_writer
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, avro_schema, payload)
    raw_avro_binary = bytes_io.getvalue()

    SCHEMA_ID = 1
    header = struct.pack(">bI", 0, SCHEMA_ID)
    final_payload = header + raw_avro_binary

    # 6. Stream the binary chunk to Kafka
    producer.produce(TOPIC_NAME, value=final_payload, callback=delivery_report)
    producer.flush()

except Exception as e:
    print(f"❌ Validation or Serialization Error: {e}")
