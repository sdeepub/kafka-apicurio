"""Simulates the machine's own sensors/PLC: publishes pure sensor readings (no part/run/recipe
context at all) to machine-telemetry, on its own 1Hz clock, independent of the context simulator."""
import time
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"
TOPIC_TELEMETRY = "machine-telemetry"

DEVICES = ["Press-A-01", "Press-A-02", "Press-A-03", "Press-B-04", "Press-B-05"]

def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=5).topics
    if TOPIC_TELEMETRY not in existing:
        admin.create_topics([NewTopic(TOPIC_TELEMETRY, num_partitions=1, replication_factor=1)])
        time.sleep(2)

def register_and_get_serializer():
    with open("local-machine-schema.avsc", "r") as f:
        schema_str = f.read()
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    sr_client.register_schema("machine-telemetry-value", Schema(schema_str, schema_type="AVRO"))
    print("✅ machine-telemetry-value schema registered.")
    return AvroSerializer(sr_client, schema_str)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"⚡ {msg.topic()} | key={msg.key().decode('utf-8')}")

def main():
    ensure_topic()
    serializer = register_and_get_serializer()
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "telemetry-simulation-engine"})
    string_serializer = StringSerializer("utf_8")

    print("\n🚀 Starting telemetry simulation loop. Press Ctrl+C to stop.\n")
    try:
        while True:
            for device in DEVICES:
                payload = {
                    "device_id": device,
                    "timestamp": int(time.time() * 1000),
                    "vibration": round(random.uniform(1.2, 8.5), 2),
                    "temperature": round(random.uniform(65.0, 110.0), 2),
                    "pressure": round(random.uniform(45.0, 95.0), 2),
                    "status": "RUNNING" if random.random() > 0.03 else "MAINTENANCE",
                }
                ctx = SerializationContext(TOPIC_TELEMETRY, MessageField.VALUE)
                producer.produce(
                    topic=TOPIC_TELEMETRY,
                    key=string_serializer(device),
                    value=serializer(payload, ctx),
                    on_delivery=delivery_report,
                )
            producer.flush(0)
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopping telemetry simulation cleanly...")
    finally:
        print("Telemetry simulator terminated safely.")

if __name__ == "__main__":
    main()
