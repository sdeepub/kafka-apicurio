package local.home.arpa.telemetry.serde;

import java.util.Map;

public class TelemetryMessage {
    public byte[] raw;                    // set only when parsing failed (goes to DLQ)
    public Map<String, String> iotdbFields;  // IoTDB "device/timestamp/measurements/types/values" map
    public Map<String, Double> numericFields; // measurement name -> value, numeric fields only, for limit checks
    public String deviceId;               // raw device id, e.g. "Press-01" (no prefix/underscore change)
    public long timestamp;

    public TelemetryMessage() {}

    public static TelemetryMessage good(Map<String, String> iotdbFields, Map<String, Double> numericFields,
                                         String deviceId, long timestamp) {
        TelemetryMessage m = new TelemetryMessage();
        m.iotdbFields = iotdbFields;
        m.numericFields = numericFields;
        m.deviceId = deviceId;
        m.timestamp = timestamp;
        return m;
    }

    public static TelemetryMessage bad(byte[] raw) {
        TelemetryMessage m = new TelemetryMessage();
        m.raw = raw;
        return m;
    }
}
