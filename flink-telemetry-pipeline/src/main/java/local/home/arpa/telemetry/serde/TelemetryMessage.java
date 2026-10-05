package local.home.arpa.telemetry.serde;

import java.util.HashMap;
import java.util.Map;

/**
 * Decoded sensor reading. sensorValues/sensorTypes hold EVERY decoded field (numbers and text
 * alike) as IoTDB-ready strings + type tags - this is what used to be flattened straight into
 * a CSV blob inside the old deserializer. It's kept as maps now, not a blob, because the final
 * IoTDB row can't be built until AFTER context enrichment (run_id/part_no/...) has had a chance
 * to attach. numericFields is a numeric-only subset, used for limit/SPC checks.
 *
 * runId/partNo/recipeName/trackIn/contextComplete are NOT set at decode time - they're filled
 * in later by ContextEnrichmentFunction, which is why they have no constructor/factory here.
 */
public class TelemetryMessage {
    public byte[] raw;                 // set only when parsing failed (goes to DLQ)
    public String deviceId;
    public long timestamp;
    public Map<String, String> sensorValues = new HashMap<>();
    public Map<String, String> sensorTypes = new HashMap<>();
    public Map<String, Double> numericFields = new HashMap<>();

    // Filled in by ContextEnrichmentFunction, not SafeDeserializer.
    public String runId;
    public String partNo;
    public String recipeName;
    public Long trackIn;
    public boolean contextComplete;

    public TelemetryMessage() {}

    public static TelemetryMessage bad(byte[] raw) {
        TelemetryMessage m = new TelemetryMessage();
        m.raw = raw;
        return m;
    }
}
