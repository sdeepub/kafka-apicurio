package local.home.arpa.telemetry.serde;

/** A decoded TRACK_IN / TRACK_OUT (or future, currently-unrecognized) event for one device. */
public class ContextEvent {
    public String deviceId;
    public String eventType;   // "TRACK_IN" | "TRACK_OUT" today; unrecognized values are ignored downstream, not errors
    public long eventTimestamp;
    public String runId;
    public String partNo;
    public String recipeName;

    public ContextEvent() {}
}
