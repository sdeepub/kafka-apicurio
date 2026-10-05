"""Simulates the scanner/MES system: publishes TRACK_IN/TRACK_OUT events to
machine-context-events, independently of the telemetry simulator's own clock. Includes a
randomized idle gap between parts - this is what produces realistic "no open context" windows
in telemetry, the same scenario discussed as a normal (not error) condition."""
import time
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"
TOPIC_CONTEXT = "machine-context-events"

DEVICES = ["Press-A-01", "Press-A-02", "Press-A-03", "Press-B-04", "Press-B-05"]
RECIPES = ["RECIPE_STD", "RECIPE_HP", "RECIPE_FAST"]

def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=5).topics
    if TOPIC_CONTEXT not in existing:
        # Deliberately NOT compacted - this is an event log (TRACK_IN/TRACK_OUT history),
        # not current-state-per-key like machine-control.
        admin.create_topics([NewTopic(TOPIC_CONTEXT, num_partitions=1, replication_factor=1)])
        time.sleep(2)

def register_and_get_serializer():
    with open("local-machine-context.avsc", "r") as f:
        schema_str = f.read()
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    sr_client.register_schema("machine-context-events-value", Schema(schema_str, schema_type="AVRO"))
    print("✅ machine-context-events-value schema registered.")
    return AvroSerializer(sr_client, schema_str)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"🔧 {msg.topic()} | key={msg.key().decode('utf-8')}")

def main():
    ensure_topic()
    serializer = register_and_get_serializer()
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "context-simulation-engine"})
    string_serializer = StringSerializer("utf_8")

    run_counters = {d: 1001 for d in DEVICES}
    part_idx = {d: 0 for d in DEVICES}
    # Per-device scheduler: each device independently alternates TRACK_IN -> (cycle) -> TRACK_OUT -> (idle gap) -> TRACK_IN...
    next_event_due = {d: time.time() + random.uniform(0, 2) for d in DEVICES}
    next_event_type = {d: "TRACK_IN" for d in DEVICES}

    print("\n🚀 Starting context-event simulation loop. Press Ctrl+C to stop.\n")
    try:
        while True:
            now = time.time()
            for device in DEVICES:
                if now < next_event_due[device]:
                    continue

                event_type = next_event_type[device]
                now_ms = int(now * 1000)

                if event_type == "TRACK_IN":
                    part_idx[device] += 1
                    if part_idx[device] > 50:
                        run_counters[device] += 1
                        part_idx[device] = 1
                    payload = {
                        "device_id": device,
                        "event_type": "TRACK_IN",
                        "event_timestamp": now_ms,
                        "run_id": f"RUN-{device}-{run_counters[device]}",
                        "part_no": f"PART-{device}-{run_counters[device]}-{part_idx[device]:04d}",
                        "recipe_name": random.choice(RECIPES),
                    }
                    cycle_duration = random.uniform(4.0, 6.0)
                    next_event_due[device] = now + cycle_duration
                    next_event_type[device] = "TRACK_OUT"
                else:  # TRACK_OUT
                    payload = {
                        "device_id": device,
                        "event_type": "TRACK_OUT",
                        "event_timestamp": now_ms,
                        "run_id": None,
                        "part_no": None,
                        "recipe_name": None,
                    }
                    idle_gap = random.uniform(0.5, 2.0)  # realistic "no open context" window
                    next_event_due[device] = now + idle_gap
                    next_event_type[device] = "TRACK_IN"

                ctx = SerializationContext(TOPIC_CONTEXT, MessageField.VALUE)
                producer.produce(
                    topic=TOPIC_CONTEXT,
                    key=string_serializer(device),
                    value=serializer(payload, ctx),
                    on_delivery=delivery_report,
                )

            producer.flush(0)
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping context simulation cleanly...")
    finally:
        print("Context simulator terminated safely.")

if __name__ == "__main__":
    main()
