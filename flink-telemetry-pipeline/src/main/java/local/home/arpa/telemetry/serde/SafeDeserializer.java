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

/**
 * Decodes PURE SENSOR telemetry (Confluent wire format), reader schema fetched from the
 * registry at startup - no schema hardcoded here. Unlike the earlier version of this class,
 * it does NOT build an IoTDB device path or flatten fields into a CSV blob: that now happens
 * downstream, after context enrichment, because the final row needs run_id/part_no/recipe_name
 * which aren't known at decode time.
 *
 * One instance of this class is constructed per machine-family topic (see TelemetryPipelineJob);
 * the "topic" constructor derives subject = topic + "-value".
 */
public class SafeDeserializer implements KafkaRecordDeserializationSchema<TelemetryMessage> {

    private final String schemaRegistryUrl;
    private final String subject;
    private final String deviceIdField;
    private final String timestampField;

    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avro;

    public SafeDeserializer(String schemaRegistryUrl, String topic) {
        this(schemaRegistryUrl, topic + "-value", "device_id", "timestamp");
    }

    public SafeDeserializer(String schemaRegistryUrl, String subject, String deviceIdField, String timestampField) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subject = subject;
        this.deviceIdField = deviceIdField;
        this.timestampField = timestampField;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        Schema readerSchema = SchemaRegistryLookup.fetchLatest(schemaRegistryUrl, subject);
        this.avro = ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TelemetryMessage> out) throws IOException {
        byte[] raw = record.value();
        if (raw == null) {
            return; // tombstone
        }
        try {
            GenericRecord r = avro.deserialize(raw);
            out.collect(buildMessage(r));
        } catch (Exception e) {
            out.collect(TelemetryMessage.bad(raw));
        }
    }

    private TelemetryMessage buildMessage(GenericRecord r) {
        Schema schema = r.getSchema();

        Object deviceIdRaw = r.get(deviceIdField);
        Object timestampRaw = r.get(timestampField);
        if (deviceIdRaw == null || timestampRaw == null) {
            throw new IllegalArgumentException(
                    "Record is missing required field '" + deviceIdField + "' or '" + timestampField + "'");
        }

        TelemetryMessage m = new TelemetryMessage();
        m.deviceId = deviceIdRaw.toString();
        m.timestamp = ((Number) timestampRaw).longValue();

        for (Schema.Field f : schema.getFields()) {
            String n = f.name();
            if (n.equals(deviceIdField) || n.equals(timestampField)) continue;

            Object v = r.get(n);
            String t = iotdbType(f.schema());
            if (v == null || t == null) continue;

            m.sensorValues.put(n, v.toString().replace(',', ';')); // the IoTDB connector splits on ','
            m.sensorTypes.put(n, t);
            if (v instanceof Number) {
                m.numericFields.put(n, ((Number) v).doubleValue());
            }
        }
        return m;
    }

    private static String iotdbType(Schema s) {
        if (s.getType() == Schema.Type.UNION) {
            for (Schema b : s.getTypes()) if (b.getType() != Schema.Type.NULL) s = b;
        }
        switch (s.getType()) {
            case STRING:  return "TEXT";
            case INT:     return "INT32";
            case LONG:    return "INT64";
            case FLOAT:   return "FLOAT";
            case DOUBLE:  return "DOUBLE";
            case BOOLEAN: return "BOOLEAN";
            default:      return null;
        }
    }

    @Override
    public TypeInformation<TelemetryMessage> getProducedType() {
        return TypeInformation.of(TelemetryMessage.class);
    }
}
