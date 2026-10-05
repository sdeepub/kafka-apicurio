package local.home.arpa.telemetry.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/** Decodes machine-context-events the same way telemetry/control are decoded: reader schema
 *  fetched from the registry at startup, no schema hardcoded here. */
public class ContextEventDeserializer implements KafkaRecordDeserializationSchema<ContextEventMessage> {

    private final String schemaRegistryUrl;
    private final String subject;
    private final String deviceIdField;
    private final String eventTypeField;
    private final String eventTimestampField;
    private final String runIdField;
    private final String partNoField;
    private final String recipeNameField;

    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avro;

    public ContextEventDeserializer(String schemaRegistryUrl, String topic) {
        this(schemaRegistryUrl, topic + "-value", "device_id", "event_type", "event_timestamp",
                "run_id", "part_no", "recipe_name");
    }

    public ContextEventDeserializer(String schemaRegistryUrl, String subject, String deviceIdField,
                                     String eventTypeField, String eventTimestampField,
                                     String runIdField, String partNoField, String recipeNameField) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subject = subject;
        this.deviceIdField = deviceIdField;
        this.eventTypeField = eventTypeField;
        this.eventTimestampField = eventTimestampField;
        this.runIdField = runIdField;
        this.partNoField = partNoField;
        this.recipeNameField = recipeNameField;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        Schema readerSchema = SchemaRegistryLookup.fetchLatest(schemaRegistryUrl, subject);
        this.avro = ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ContextEventMessage> out) throws IOException {
        byte[] raw = record.value();
        if (raw == null) return; // tombstone
        try {
            GenericRecord r = avro.deserialize(raw);
            ContextEvent e = new ContextEvent();
            e.deviceId = String.valueOf(r.get(deviceIdField));
            e.eventType = String.valueOf(r.get(eventTypeField));
            e.eventTimestamp = ((Number) r.get(eventTimestampField)).longValue();
            e.runId = r.get(runIdField) != null ? r.get(runIdField).toString() : null;
            e.partNo = r.get(partNoField) != null ? r.get(partNoField).toString() : null;
            e.recipeName = r.get(recipeNameField) != null ? r.get(recipeNameField).toString() : null;
            out.collect(ContextEventMessage.good(e));
        } catch (Exception ex) {
            out.collect(ContextEventMessage.bad(raw));
        }
    }

    @Override
    public TypeInformation<ContextEventMessage> getProducedType() {
        return TypeInformation.of(ContextEventMessage.class);
    }
}
