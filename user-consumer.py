import io
import requests
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader

# 1. Configuration Setup
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "user-events"
DLQ_TOPIC_NAME = "user-events-dlq"
CONSUMER_GROUP = "python-avro-group"
APICURIO_URL = "http://localhost:8080/apis/registry/v3/groups/default/artifacts/13e47313-0fd5-4a3e-8eb8-cd6c0bc4d3d3/versions/1/content"

print("🔄 Connecting to Apicurio to pull data schema...")
response = requests.get(APICURIO_URL)
if response.status_code != 200:
    print(f"❌ Failed to fetch schema. Status code: {response.status_code}")
    exit(1)

# Parse the schema layout natively via fastavro
avro_schema = parse_schema(response.json())
print("✅ Schema layout loaded successfully!")

# 2. Initialize the Kafka Consumer Client
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest" # Read the topic stream from the beginning
})

consumer.subscribe([TOPIC_NAME])

dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

print(f"🎧 Consumer active. Listening to topic '{TOPIC_NAME}'...")

try:
    while True:
        # Poll Kafka for incoming records (timeout in seconds)
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Kafka Error encountered: {msg.error()}")
                break

        # 3. Handle data incoming from the stream
        raw_bytes = msg.value()
        
        try:
            # Check if the message starts with the 5-byte Confluent header
            # (Magic Byte is 0x00, total length must be greater than 5)
            if len(raw_bytes) > 5 and raw_bytes[0] == 0:
                # Slice off the first 5 bytes (1 magic byte + 4 schema ID bytes)
                # and only pass the actual data payload to the reader
                actual_avro_bytes = raw_bytes[5:]
            else:
                # Fallback for old messages that didn't have a header
                actual_avro_bytes = raw_bytes

            # Native Deserialization Loop:
            bytes_io = io.BytesIO(actual_avro_bytes)
            decoded_record = schemaless_reader(bytes_io, avro_schema)
            
            print(f"📥 Received Record [Offset {msg.offset()}]: {decoded_record}")

        except Exception as serialization_error:
            print(f"⚠️ Caught a bad message packet at Offset {msg.offset()}: {serialization_error}")
            print(f"⏩ Rerouting raw toxic bytes to DLQ topic '{DLQ_TOPIC_NAME}'...")

            # Send the unmodified raw byte sequence to the triage topic
            dlq_producer.produce(
                topic=DLQ_TOPIC_NAME,
                value=raw_bytes,
                headers=[("error_message", str(serialization_error).encode('utf-8'))]
            )
            dlq_producer.flush() # Ensure the write triggers instantly        

except KeyboardInterrupt:
    print("\n🛑 Shutting down consumer nicely...")
finally:
    consumer.close()
