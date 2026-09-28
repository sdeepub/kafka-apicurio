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

public class SafeDeserializer implements KafkaRecordDeserializationSchema<TelemetryMessage> {

    private static final String DEVICE_PREFIX = "root.local.home.arpa.press.";

    // Reader schema: identical to local-machine-schema.avsc (the schema the producer registers).
    // The reader schema decides which fields survive, so keep it in sync with that file.
    private static final String SCHEMA_JSON =
        "{\"type\":\"record\",\"name\":\"MachineTelemetry\",\"namespace\":\"local.home.arpa.telemetry\",\"fields\":["
      + "{\"name\":\"device_id\",\"type\":\"string\"},"
      + "{\"name\":\"timestamp\",\"type\":\"long\"},"
      + "{\"name\":\"vibration\",\"type\":\"double\"},"
      + "{\"name\":\"temperature\",\"type\":\"double\"},"
      + "{\"name\":\"pressure\",\"type\":\"double\"},"
      + "{\"name\":\"status\",\"type\":\"string\"},"
      + "{\"name\":\"track_in\",\"type\":\"long\"},"
      + "{\"name\":\"track_out\",\"type\":\"long\"},"
      + "{\"name\":\"run_num\",\"type\":\"int\"},"
      + "{\"name\":\"part_id\",\"type\":\"string\"}"
      + "]}";

    private final String schemaRegistryUrl;
    private transient Schema schema;
    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avro;

    public SafeDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        this.schema = new Schema.Parser().parse(SCHEMA_JSON);
        this.avro = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TelemetryMessage> out) throws IOException {
        byte[] raw = record.value();
        if (raw == null) {
            return; // tombstone: nothing to parse, nothing worth a DLQ entry
        }
        try {
            GenericRecord r = avro.deserialize(raw);
            out.collect(TelemetryMessage.good(toIoTDBMap(r)));
        } catch (Exception e) {
            // bad bytes, unknown schema id, missing/mistyped field, etc. -> DLQ
            out.collect(TelemetryMessage.bad(raw));
        }
    }

    /**
     * DefaultIoTSerializationSchema expects exactly these keys:
     *   device, timestamp, measurements ("a,b,c"), types ("DOUBLE,TEXT,INT32"), values ("1.0,ok,7")
     */
    private Map<String, String> toIoTDBMap(GenericRecord r) {
        String device = DEVICE_PREFIX + r.get("device_id").toString().replace("-", "_");
        long ts = ((Number) r.get("timestamp")).longValue();

        StringBuilder names = new StringBuilder();
        StringBuilder types = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (Schema.Field f : schema.getFields()) {
            String n = f.name();
            if (n.equals("device_id") || n.equals("timestamp")) continue;
            Object v = r.get(n);
            String t = iotdbType(f.schema());
            if (v == null || t == null) continue;

            if (names.length() > 0) { names.append(','); types.append(','); values.append(','); }
            names.append(n);
            types.append(t);
            values.append(v.toString().replace(',', ';')); // the connector splits on ','
        }

        Map<String, String> m = new HashMap<>();
        m.put("device", device);
        m.put("timestamp", String.valueOf(ts));
        m.put("measurements", names.toString());
        m.put("types", types.toString());
        m.put("values", values.toString());
        return m;
    }

    private static String iotdbType(Schema s) {
        if (s.getType() == Schema.Type.UNION) {            // e.g. ["null","double"]
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
