package local.home.arpa.telemetry.serde;

public class ContextEventMessage {
    public byte[] raw;        // set only when parsing failed (goes to DLQ)
    public ContextEvent event; // set only on success

    public ContextEventMessage() {}

    public static ContextEventMessage good(ContextEvent event) {
        ContextEventMessage m = new ContextEventMessage();
        m.event = event;
        return m;
    }

    public static ContextEventMessage bad(byte[] raw) {
        ContextEventMessage m = new ContextEventMessage();
        m.raw = raw;
        return m;
    }
}
