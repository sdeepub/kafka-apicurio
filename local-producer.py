import time
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# 1. Pipeline Routing Configurations
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"

TOPIC_TELEMETRY = "machine-telemetry"
TOPIC_CONTROL = "machine-control"
TOPIC_DLQ = "machine-DLQ"
TOPIC_ALERTS = "machine-alerts"
TOPIC_INCOMPLETE_TRACK = "machine-incomplete-track"

def init_kafka_topics():
    """Ensures all five operational Kafka topics exist in the broker with connection retries."""
    print("Connecting to Kafka Broker and validating topics...")
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    existing_topics = None
    for attempt in range(1, 6):
        try:
            existing_topics = admin_client.list_topics(timeout=3).topics
            break
        except Exception:
            print(f"⏳ [Attempt {attempt}/5] Kafka metadata not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    if existing_topics is None:
        print(f"❌ Error: Could not establish a stable metadata session at {BOOTSTRAP_SERVERS}.")
        print("Ensure your Docker containers are completely up ('docker compose ps').")
        exit(1)

    required_topics = [TOPIC_TELEMETRY, TOPIC_CONTROL, TOPIC_DLQ, TOPIC_ALERTS, TOPIC_INCOMPLETE_TRACK]
    new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in required_topics if t not in existing_topics]

    if new_topics:
        futures = admin_client.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"✅ Topic '{topic}' created successfully.")
            except Exception as e:
                print(f"❌ Failed to create topic '{topic}': {e}")
    else:
        print("Existing topics validated. All required streams are present.")

def register_and_get_serializers():
    """Loads schemas, registers them to Apicurio, and returns compiled serializers.
    Subjects follow the standard '<topic>-value' convention so Flink's registry lookups
    (which also follow that convention) resolve without any manual renaming in the UI."""
    print("Registering Avro schemas in Apicurio Schema Registry...")

    try:
        with open("local-machine-schema.avsc", "r") as f:
            telemetry_schema_str = f.read()
    except FileNotFoundError:
        print("❌ Error: 'local-machine-schema.avsc' not found.")
        exit(1)

    try:
        with open("local-machine-control.avsc", "r") as f:
            control_schema_str = f.read()
    except FileNotFoundError:
        print("❌ Error: 'local-machine-control.avsc' not found.")
        exit(1)

    try:
        sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        telemetry_schema = Schema(telemetry_schema_str, schema_type="AVRO")
        control_schema = Schema(control_schema_str, schema_type="AVRO")

        sr_client.register_schema("machine-telemetry-value", telemetry_schema)
        print("✅ machine-telemetry-value schema verified/registered.")

        sr_client.register_schema("machine-control-value", control_schema)
        print("✅ machine-control-value schema verified/registered.")

        telemetry_serializer = AvroSerializer(sr_client, telemetry_schema_str)
        control_serializer = AvroSerializer(sr_client, control_schema_str)

        return telemetry_serializer, control_serializer
    except Exception as e:
        print(f"❌ Error communicating with Apicurio Registry at {SCHEMA_REGISTRY_URL}: {e}")
        exit(1)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"⚡ Streamed -> {msg.topic()} [{msg.partition()}] offset {msg.offset()} | Key: {msg.key().decode('utf-8')}")

def main():
    init_kafka_topics()
    telemetry_serializer, control_serializer = register_and_get_serializers()

    producer_config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'press-simulation-engine',
        'acks': '1'
    }
    producer = Producer(producer_config)
    string_serializer = StringSerializer('utf_8')

    # Device names now ENCODE their type ("Press-A", "Press-B") - this matters because Flink
    # derives a device's type by stripping its trailing "-NN" number (Press-A-01 -> "Press-A").
    # Flat names like the old "Press-01".."Press-05" would all resolve to the same type "Press",
    # making group_alpha and group_beta indistinguishable to the pipeline.
    group_alpha = ["Press-A-01", "Press-A-02", "Press-A-03"]
    group_beta = ["Press-B-04", "Press-B-05"]
    all_devices = group_alpha + group_beta

    run_counters = {device: 1001 for device in all_devices}
    part_idx = {device: 0 for device in all_devices}

    # One control-rule record PER MACHINE TYPE (not per device) - this is the point of grouping:
    # new presses of an existing type need no new control-rule record at all.
    control_rules_by_type = {
        "Press-A": {
            "version": "v1.0.0",
            "machine_type": "Press-A",
            "max_vibration": 5.50,
            "min_vibration": 1.50,
            "max_temperature": 90.00,
            "min_temperature": 70.00,
            "max_pressure": 75.00,
            "min_pressure": 55.00,
            "rule_profile": "HIGH_PRECISION_STRICT"
        },
        "Press-B": {
            "version": "v1.0.0",
            "machine_type": "Press-B",
            "max_vibration": 8.50,
            "min_vibration": 1.00,
            "max_temperature": 115.00,
            "min_temperature": 60.00,
            "max_pressure": 95.00,
            "min_pressure": 45.00,
            "rule_profile": "STANDARD_HEAVY_DUTY"
        }
    }

    print("\n📦 Publishing Machine Control Boundary Profiles (one record per TYPE)...")
    for machine_type, control_payload in control_rules_by_type.items():
        control_context = SerializationContext(TOPIC_CONTROL, MessageField.VALUE)
        producer.produce(
            topic=TOPIC_CONTROL,
            key=string_serializer(machine_type),
            value=control_serializer(control_payload, control_context),
            on_delivery=delivery_report
        )
    producer.flush()
    print("✅ Control configuration boundaries successfully broadcasted.")

    print("\n🚀 Starting active machine-telemetry simulation loop. Press Ctrl+C to stop.\n")

    try:
        while True:
            for device in all_devices:
                part_idx[device] += 1
                if part_idx[device] > 50:
                    run_counters[device] += 1
                    part_idx[device] = 1

                current_time_ms = int(time.time() * 1000)
                processing_duration_ms = random.randint(4000, 6000)

                telemetry_payload = {
                    "device_id": device,
                    "timestamp": current_time_ms,
                    "vibration": round(random.uniform(1.2, 8.5), 2),
                    "temperature": round(random.uniform(65.0, 110.0), 2),
                    "pressure": round(random.uniform(45.0, 95.0), 2),
                    "status": "RUNNING" if random.random() > 0.03 else "MAINTENANCE",
                    "track_in": current_time_ms - processing_duration_ms,
                    "track_out": current_time_ms,
                    "run_num": run_counters[device],
                    "part_id": f"PART-{device}-{run_counters[device]}-{part_idx[device]:04d}"
                }

                telemetry_context = SerializationContext(TOPIC_TELEMETRY, MessageField.VALUE)
                producer.produce(
                    topic=TOPIC_TELEMETRY,
                    key=string_serializer(device),
                    value=telemetry_serializer(telemetry_payload, telemetry_context),
                    on_delivery=delivery_report
                )

            producer.flush()
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopping data generation loop cleanly...")
    finally:
        print("Producer terminated safely.")

if __name__ == "__main__":
    main()
