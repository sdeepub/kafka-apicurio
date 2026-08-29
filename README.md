### Getting Started & Initialization Protocol

Follow this checklist to clone, initialize, and spin up the complete distributed streaming pipeline.

### 1. Re-Download Heavy Binary SQL Connectors
Due to storage constraint tracking, heavy compiled Java class libraries are omitted from Git tracking matrices. Download the verified fat JAR files directly into your project path:

```bash
curl -L -o ./flink-sql-connector-kafka-3.1.0-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

curl -L -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.18.1/flink-sql-avro-confluent-registry-1.18.1.jar
```

### 2. Initialize the Python Virtual Environment
Build an isolated sandbox dependencies cluster for your local execution processes:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install confluent-kafka fastavro requests apicurio-registry-sdk
```

### 3. Spin Up the Core Distributed Systems Infrastructure
Use Docker Compose to launch your cluster container ecosystem services in the background:

```bash
docker compose up -d
```
*Verify that all containers (Kafka, Apicurio Backend, Apicurio UI, Kafbat UI, Flink JobManager, Flink TaskManager) are running stably with `docker ps`.*

---

## Verification and Runtime Execution

### Step 1: Upload the Telemetry Contract Template
1. Open your web browser and navigate to the Apicurio Visual Web Console: `http://localhost:8888`.
2. Click **Upload Artifact**. Set the **Group** parameter to `default` and input the **Artifact ID** exactly as: `481ebcd6-801a-4c18-8915-8ededc077d41`.
3. Choose **AVRO** as the document type schema format and paste the tracking matrix into the template canvas:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```
4. Click **Upload**.

### Step 2: Activate the Live Stream Listener (Consumer)
Open a dedicated terminal tab, activate your virtual environment sandbox, and spin up the backend reader script:
```bash
source .venv/bin/activate
python3 consumer.py
```

### Step 3: Trigger the Event Stream Engine (Producer)
Open a separate terminal tab, activate your virtual sandbox, and generate a validated payload event:
```bash
source .venv/bin/activate
python3 producer.py
```
*The producer will fetch the schema layout, compress data down to raw binary byte structures, prepend the 5-byte identifier metadata, and fire. The consumer window will instantly capture the record offset partition line!*

### Step 4: Run Real-Time Streaming Analytics inside Apache Flink
1. Access Flink's interactive query interface:
   ```bash
   docker exec -it flink-jobmanager ./bin/sql-client.sh
   ```
2. Register the streaming data table layout map parameters inside the SQL console prompt:
   ```sql
   CREATE TABLE user_stream (
       id STRING,
       name STRING,
       email STRING
   ) WITH (
       'connector' = 'kafka',
       'topic' = 'user-events',
       'properties.bootstrap.servers' = 'kafka-broker:9094',
       'scan.startup.mode' = 'earliest-offset',
       'format' = 'avro-confluent',
       'avro-confluent.schema-registry.url' = 'http://apicurio-registry:8080/apis/ccompat/v7'
   );
   ```
3. Initialize the continuous real-time relational analytics engine tracker:
   ```sql
   SELECT * FROM user_stream;
   ```
*Fire your `producer.py` script again—the values will populate across the analytical Flink processing screen in real time!*

---

### Cluster Teardown Protocol
To clear out temporary container structures and safely free up your machine's system memory resources when you are finished testing:

```bash
# 1. Press Q then type QUIT; inside Flink SQL to exit the client shell.
# 2. Press Ctrl+C inside your python app tabs, then call:
deactivate

# 3. Destroy background infrastructure services cleanly
docker compose down
```
