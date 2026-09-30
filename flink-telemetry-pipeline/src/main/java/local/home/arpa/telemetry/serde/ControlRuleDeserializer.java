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
 * Decodes machine-control-rules Avro records the same way SafeDeserializer decodes telemetry:
 * reader schema fetched from the registry at startup, fields discovered dynamically. Any
 * numeric field named "max_<measurement>" becomes an upper limit; "min_<measurement>" becomes
 * a lower limit.
 *
 * Each record covers one MACHINE TYPE (e.g. "Press-A"), read from machineTypeField
 * (default "machine_type") - not an individual device.
 */
public class ControlRuleDeserializer implements KafkaRecordDeserializationSchema<ControlRuleMessage> {

    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";

    private final String schemaRegistryUrl;
    private final String subject;
    private final String machineTypeField;
    private final String versionField;
    private final String ruleProfileField;

    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avro;

    public ControlRuleDeserializer(String schemaRegistryUrl, String topic) {
        this(schemaRegistryUrl, topic + "-value", "machine_type", "version", "rule_profile");
    }

    public ControlRuleDeserializer(String schemaRegistryUrl, String subject, String machineTypeField,
                                    String versionField, String ruleProfileField) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subject = subject;
        this.machineTypeField = machineTypeField;
        this.versionField = versionField;
        this.ruleProfileField = ruleProfileField;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        Schema readerSchema = SchemaRegistryLookup.fetchLatest(schemaRegistryUrl, subject);
        this.avro = ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ControlRuleMessage> out) throws IOException {
        byte[] raw = record.value();
        if (raw == null) {
            return; // tombstone (e.g. a compacted delete) - nothing to route
        }
        try {
            GenericRecord r = avro.deserialize(raw);
            out.collect(ControlRuleMessage.good(buildRule(r)));
        } catch (Exception e) {
            out.collect(ControlRuleMessage.bad(raw));
        }
    }

    private ControlRule buildRule(GenericRecord r) {
        ControlRule rule = new ControlRule();
        rule.machineType = String.valueOf(r.get(machineTypeField));
        rule.version = r.get(versionField) != null ? r.get(versionField).toString() : null;
        rule.ruleProfile = r.get(ruleProfileField) != null ? r.get(ruleProfileField).toString() : null;

        for (Schema.Field f : r.getSchema().getFields()) {
            String n = f.name();
            Object v = r.get(n);
            if (!(v instanceof Number)) continue;

            if (n.startsWith(MAX_PREFIX)) {
                rule.upperLimits.put(n.substring(MAX_PREFIX.length()), ((Number) v).doubleValue());
            } else if (n.startsWith(MIN_PREFIX)) {
                rule.lowerLimits.put(n.substring(MIN_PREFIX.length()), ((Number) v).doubleValue());
            }
        }
        return rule;
    }

    @Override
    public TypeInformation<ControlRuleMessage> getProducedType() {
        return TypeInformation.of(ControlRuleMessage.class);
    }
}
