package local.home.arpa.telemetry.serde;

import java.util.Map;

/**
 * Carrier between the Kafka source and the router.
 * Flink "POJO" rules: public class, public no-arg constructor, public fields.
 * Exactly one of the two fields is non-null.
 */
public class TelemetryMessage {
    public byte[] raw;                 // set only when the record could NOT be parsed (goes to DLQ)
    public Map<String, String> fields; // set only when parsed; already in IoTDB "device/timestamp/measurements/types/values" form

    public TelemetryMessage() {}

    public static TelemetryMessage good(Map<String, String> fields) {
        TelemetryMessage m = new TelemetryMessage();
        m.fields = fields;
        return m;
    }

    public static TelemetryMessage bad(byte[] raw) {
        TelemetryMessage m = new TelemetryMessage();
        m.raw = raw;
        return m;
    }
}
