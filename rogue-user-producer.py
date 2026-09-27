import struct
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "user-events"

print("😈 Initializing rogue producer...")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Text that completely violates the registered Avro Schema block
toxic_payload = b"This is a completely invalid raw text packet string!"

# Spoofing the 5-byte Confluent wire format header (Magic Byte 0 + Schema ID 1)
SCHEMA_ID = 1
header = struct.pack(">bI", 0, SCHEMA_ID)
final_payload = header + toxic_payload

print("🚀 Launching poison pill straight into Kafka...")
producer.produce(TOPIC_NAME, value=final_payload)
producer.flush()
print("💀 Poison pill delivered.")
