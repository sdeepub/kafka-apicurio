"""Publishes control-rule records (one per machine TYPE) to machine-control. One-shot - this
represents a config push, not a continuous stream. Run again whenever limits change."""
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"
TOPIC_CONTROL = "machine-control"

CONTROL_RULES_BY_TYPE = {
    "Press-A": {
        "version": "v1.0.0", "machine_type": "Press-A",
        "max_vibration": 5.50, "min_vibration": 1.50,
        "max_temperature": 90.00, "min_temperature": 70.00,
        "max_pressure": 75.00, "min_pressure": 55.00,
        "rule_profile": "HIGH_PRECISION_STRICT",
    },
    "Press-B": {
        "version": "v1.0.0", "machine_type": "Press-B",
        "max_vibration": 8.50, "min_vibration": 1.00,
        "max_temperature": 115.00, "min_temperature": 60.00,
        "max_pressure": 95.00, "min_pressure": 45.00,
        "rule_profile": "STANDARD_HEAVY_DUTY",
    },
}

def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=5).topics
    if TOPIC_CONTROL not in existing:
        # NOT compacted here on purpose by this script - set cleanup.policy=compact on this
        # topic once, by hand (or in your provisioning), since it holds current-state-per-key.
        admin.create_topics([NewTopic(TOPIC_CONTROL, num_partitions=1, replication_factor=1)])
        time.sleep(2)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"⚡ {msg.topic()} | key={msg.key().decode('utf-8')}")

def main():
    ensure_topic()
    with open("local-machine-control.avsc", "r") as f:
        schema_str = f.read()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    sr_client.register_schema("machine-control-value", Schema(schema_str, schema_type="AVRO"))
    print("✅ machine-control-value schema registered.")

    serializer = AvroSerializer(sr_client, schema_str)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "control-publisher"})
    string_serializer = StringSerializer("utf_8")

    for machine_type, payload in CONTROL_RULES_BY_TYPE.items():
        ctx = SerializationContext(TOPIC_CONTROL, MessageField.VALUE)
        producer.produce(
            topic=TOPIC_CONTROL,
            key=string_serializer(machine_type),
            value=serializer(payload, ctx),
            on_delivery=delivery_report,
        )
    producer.flush()
    print("✅ Control rules published for:", list(CONTROL_RULES_BY_TYPE.keys()))

if __name__ == "__main__":
    main()
