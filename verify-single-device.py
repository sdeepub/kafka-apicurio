"""
Deterministic verification dataset for ONE device - scripted to hit every quarantine reason,
every context_event type, and a limit breach, in a known order, so the actual pipeline output
can be checked step by step rather than inferred from random data.

Uses a BRAND-NEW device id (Press-A-99) so Flink's keyed state starts genuinely empty - this is
what makes "never_tracked_since_job_start" reproducible on demand. It inherits Press-A's control
limits automatically via the machine-type naming convention (no new control-rule record needed).

Run this from the same directory as local-machine-schema.avsc and local-machine-context.avsc
(same convention as the other producer scripts). Watch Kafbat UI (machine-alerts,
machine-incomplete-track) and the IoTDB CLI while it runs - each step prints what you should
see appear.
"""
import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8080/apis/ccompat/v7"
TOPIC_TELEMETRY = "machine-telemetry"
TOPIC_CONTEXT = "machine-context-events"
DEVICE = "Press-A-99"  # fresh device; resolves to machine_type "Press-A" via naming convention

sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
with open("local-machine-schema.avsc") as f:
    telemetry_serializer = AvroSerializer(sr_client, f.read())
with open("local-machine-context.avsc") as f:
    context_serializer = AvroSerializer(sr_client, f.read())

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "verification-script"})
string_serializer = StringSerializer("utf_8")

def send_telemetry(vibration, temperature=80.0, pressure=65.0, status="RUNNING"):
    payload = {
        "device_id": DEVICE, "timestamp": int(time.time() * 1000),
        "vibration": vibration, "temperature": temperature, "pressure": pressure, "status": status,
    }
    ctx = SerializationContext(TOPIC_TELEMETRY, MessageField.VALUE)
    producer.produce(TOPIC_TELEMETRY, key=string_serializer(DEVICE),
                      value=telemetry_serializer(payload, ctx))
    producer.flush()

def send_context(event_type, run_id=None, part_no=None, recipe_name=None):
    payload = {
        "device_id": DEVICE, "event_type": event_type, "event_timestamp": int(time.time() * 1000),
        "run_id": run_id, "part_no": part_no, "recipe_name": recipe_name,
    }
    ctx = SerializationContext(TOPIC_CONTEXT, MessageField.VALUE)
    producer.produce(TOPIC_CONTEXT, key=string_serializer(DEVICE),
                      value=context_serializer(payload, ctx))
    producer.flush()

def step(n, description, expect):
    print(f"\n--- Step {n}: {description} ---")
    print(f"    expect: {expect}")

PAUSE = 2.0  # real delay so Flink's poll loop processes each context event before the next step

# --- Case: never_tracked_since_job_start ---
step(1, "3 sensor readings, no context sent yet", "3x quarantine reason=never_tracked_since_job_start, last_track_out=null")
for _ in range(3):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)

# --- Case: normal complete run (Run #1) ---
step(2, "TRACK_IN run=RUN-1", "IoTDB row context_event=TRACK_IN, data_point_count=0")
send_context("TRACK_IN", run_id="RUN-1", part_no="PART-TEST-0001", recipe_name="RECIPE_STD")
time.sleep(PAUSE)

step(3, "3 sensor readings under RUN-1", "3x readings with run_id=RUN-1, data_point_count=1,2,3")
for _ in range(3):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)

step(4, "TRACK_OUT closes RUN-1", "IoTDB row context_event=TRACK_OUT_CLOSE, data_point_count=3")
send_context("TRACK_OUT")
time.sleep(PAUSE)

# --- Case: between_cycles ---
step(5, "2 sensor readings, no cycle open", "2x quarantine reason=between_cycles (last_track_out set, small gap_ms)")
for _ in range(2):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)

# --- Case: empty run (zero data points) ---
step(6, "TRACK_IN run=RUN-2, immediately TRACK_OUT, NO readings in between", "TRACK_OUT_CLOSE row with data_point_count=0")
send_context("TRACK_IN", run_id="RUN-2", part_no="PART-TEST-0002", recipe_name="RECIPE_STD")
time.sleep(PAUSE)
send_context("TRACK_OUT")
time.sleep(PAUSE)

# --- Case: abandoned_cycle ---
step(7, "TRACK_IN run=RUN-3 (opens, never closed)", "IoTDB TRACK_IN row for RUN-3")
send_context("TRACK_IN", run_id="RUN-3", part_no="PART-TEST-0003", recipe_name="RECIPE_STD")
time.sleep(PAUSE)

step(8, "2 sensor readings under RUN-3", "2x readings with run_id=RUN-3, data_point_count=1,2")
for _ in range(2):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)

step(9, "TRACK_IN run=RUN-4 WITHOUT closing RUN-3 first", "quarantine reason=abandoned_cycle, abandoned_run_id=RUN-3, data_point_count_at_abandonment=2")
send_context("TRACK_IN", run_id="RUN-4", part_no="PART-TEST-0004", recipe_name="RECIPE_STD")
time.sleep(PAUSE)

step(10, "2 sensor readings under RUN-4, then clean TRACK_OUT", "2x readings run_id=RUN-4; TRACK_OUT_CLOSE data_point_count=2")
for _ in range(2):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)
send_context("TRACK_OUT")
time.sleep(PAUSE)

# --- Case: orphaned_track_out ---
step(11, "TRACK_OUT with nothing open (RUN-4 already closed)", "quarantine reason=orphaned_track_out; IoTDB row context_event=TRACK_OUT_ORPHANED, no run_id/part_no")
send_context("TRACK_OUT")
time.sleep(PAUSE)

# --- Case: between_cycles after an orphaned close (the documented blind spot) ---
step(12, "2 sensor readings after the orphaned TRACK_OUT", "2x quarantine reason=between_cycles - NOT never_tracked, even though the close that set last_track_out was itself orphaned. Confirms the 'trace back one step' pattern discussed.")
for _ in range(2):
    send_telemetry(vibration=3.0)
    time.sleep(0.3)
time.sleep(PAUSE)

# --- Case: limit breach ---
step(13, "TRACK_IN run=RUN-5, one reading with vibration=7.0 (Press-A max is 5.50), then TRACK_OUT", "machine-alerts: alert_type=limit_breach, limit_type=upper, limit_value=5.5; TRACK_OUT_CLOSE data_point_count=1")
send_context("TRACK_IN", run_id="RUN-5", part_no="PART-TEST-0005", recipe_name="RECIPE_HP")
time.sleep(PAUSE)
send_telemetry(vibration=7.0)
time.sleep(PAUSE)
send_context("TRACK_OUT")

print("\n=== Done. 13 steps sent for device", DEVICE, "===")
print("SPC zone-rule checks are NOT exercised here - they need 15+ consecutive readings per")
print("measurement to even start evaluating. Use the live simulators running for a few minutes")
print("on one device for that, rather than a short scripted sequence like this one.")
