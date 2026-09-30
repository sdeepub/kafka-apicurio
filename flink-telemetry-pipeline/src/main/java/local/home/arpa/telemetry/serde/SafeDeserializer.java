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
import java.util.HashMap;
import java.util.Map;

/**
 * Decodes machine-telemetry Avro records (Confluent wire format), validating against a reader
 * schema fetched from the registry at startup - no schema is hardcoded in this class. The
 * IoTDB row and the numeric measurement map are both built by walking the record's own schema,
 * so adding/removing a telemetry field requires no code change here.
 *
 * device_id / timestamp are a naming CONVENTION this pipeline relies on, not a hardcoded schema.
 */
public class SafeDeserializer implements KafkaRecordDeserializationSchema<TelemetryMessage> {

    private static final String DEVICE_PREFIX = "root.local.home.arpa.press.";

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
        String deviceId = deviceIdRaw.toString();
        long ts = ((Number) timestampRaw).longValue();
        String iotdbDevice = DEVICE_PREFIX + deviceId.replace("-", "_");

        StringBuilder names = new StringBuilder();
        StringBuilder types = new StringBuilder();
        StringBuilder values = new StringBuilder();
        Map<String, Double> numericFields = new HashMap<>();

        for (Schema.Field f : schema.getFields()) {
            String n = f.name();
            if (n.equals(deviceIdField) || n.equals(timestampField)) continue;

            Object v = r.get(n);
            String t = iotdbType(f.schema());
            if (v == null || t == null) continue;

            if (names.length() > 0) { names.append(','); types.append(','); values.append(','); }
            names.append(n);
            types.append(t);
            values.append(v.toString().replace(',', ';'));

            if (v instanceof Number) {
                numericFields.put(n, ((Number) v).doubleValue());
            }
        }

        Map<String, String> iotdbFields = new HashMap<>();
        iotdbFields.put("device", iotdbDevice);
        iotdbFields.put("timestamp", String.valueOf(ts));
        iotdbFields.put("measurements", names.toString());
        iotdbFields.put("types", types.toString());
        iotdbFields.put("values", values.toString());

        return TelemetryMessage.good(iotdbFields, numericFields, deviceId, ts);
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
