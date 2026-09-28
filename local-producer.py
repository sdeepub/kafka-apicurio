import time
import random
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# 1. Pipeline Routing Configurations
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"
TOPIC_TELEMETRY = "machine-telemetry"
TOPIC_DLQ = "machine-DLQ"
TOPIC_ALERTS = "machine-alerts"

def init_kafka_topics():
    """Ensures all three operational Kafka topics exist in the broker."""
    print("Checking and creating required Kafka topics...")
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    
    try:
        existing_topics = admin_client.list_topics(timeout=5).topics
    except Exception as e:
        print(f"❌ Error connecting to Kafka Broker at {BOOTSTRAP_SERVERS}: {e}")
        print("Ensure your Docker containers are running ('docker compose up -d').")
        exit(1)
        
    required_topics = [TOPIC_TELEMETRY, TOPIC_DLQ, TOPIC_ALERTS]
    new_topics = []
    
    for topic in required_topics:
        if topic not in existing_topics:
            print(f"Creating topic: {topic}")
            # Single partition, single replication factor ideal for local development
            new_topics.append(NewTopic(topic, num_partitions=1, replication_factor=1))
            
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

def register_and_get_serializer():
    """Loads schema and registers it against the Apicurio Schema Registry."""
    print("Registering Avro schema in Apicurio Schema Registry...")
    
    try:
        with open("local-machine-schema.avsc", "r") as f:
            schema_str = f.read()
    except FileNotFoundError:
        print("❌ Error: 'machine-schema.avsc' not found in this directory.")
        print("Please create the .avsc file first before running this script.")
        exit(1)
        
    try:
        sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        avro_serializer = AvroSerializer(
            schema_registry_client=sr_client,
            schema_str=schema_str
        )
        return avro_serializer
    except Exception as e:
        print(f"❌ Error communicating with Apicurio Registry at {SCHEMA_REGISTRY_URL}: {e}")
        exit(1)

def delivery_report(err, msg):
    """Callback triggered upon successful delivery or failure."""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"⚡ Streamed -> {msg.topic()} [{msg.partition()}] offset {msg.offset()} | Key: {msg.key().decode('utf-8')}")

def main():
    # Initialize Kafka topics and fetch schema registration
    init_kafka_topics()
    avro_serializer = register_and_get_serializer()
    
    # Initialize the producer pointing to the host-exposed listener port
    producer_config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'press-simulation-engine',
        'acks': '1' # Fast acknowledgment for local streaming loops
    }
    producer = Producer(producer_config)
    string_serializer = StringSerializer('utf_8')
    
    # Establish simulation assets (Press-01 through Press-05)
    press_devices = [f"Press-{i:02d}" for i in range(1, 6)]
    
    # Internal state tracking dictionaries for tracking-in manufacturing context
    run_counters = {device: 1001 for device in press_devices}
    part_idx = {device: 0 for device in press_devices}
    
    print("\n🚀 Starting active machine-telemetry simulation loop. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            for device in press_devices:
                # Manufacturing context logic: increment part count and rotate runs every 50 pieces
                part_idx[device] += 1
                if part_idx[device] > 50:
                    run_counters[device] += 1
                    part_idx[device] = 1
                
                current_time_ms = int(time.time() * 1000)
                processing_duration_ms = random.randint(4000, 6000) # Simulate a ~5 second cycle time
                
                # Formulate the payload adhering explicitly to your updated schema specifications
                telemetry_payload = {
                    "device_id": device,
                    "timestamp": current_time_ms,
                    "vibration": round(random.uniform(1.2, 8.5), 2),
                    "temperature": round(random.uniform(65.0, 110.0), 2),
                    "pressure": round(random.uniform(45.0, 95.0), 2),
                    "status": "RUNNING" if random.random() > 0.03 else "MAINTENANCE",
                    
                    # Pure Unix Epoch Milliseconds (Integers) for downstream time-series engines
                    "track_in": current_time_ms - processing_duration_ms,
                    "track_out": current_time_ms,
                    "run_num": run_counters[device],
                    "part_id": f"PART-{device}-{run_counters[device]}-{part_idx[device]:04d}"
                }
                
                context = SerializationContext(TOPIC_TELEMETRY, MessageField.VALUE)
                
                # Execute serialization and submit to broker partition loop
                producer.produce(
                    topic=TOPIC_TELEMETRY,
                    key=string_serializer(device),
                    value=avro_serializer(telemetry_payload, context),
                    on_delivery=delivery_report
                )
            
            # Flush changes out immediately to maintain tight real-time execution bounds
            producer.flush()
            time.sleep(1.0) # Yield metrics once per second across all 5 assets
            
    except KeyboardInterrupt:
        print("\nStopping data generation loop cleanly...")
    finally:
        print("Producer terminated safely.")

if __name__ == "__main__":
    main()
