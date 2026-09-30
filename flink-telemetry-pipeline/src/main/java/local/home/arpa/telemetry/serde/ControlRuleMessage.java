package local.home.arpa.telemetry.serde;

public class ControlRuleMessage {
    public byte[] raw;       // set only when parsing failed (goes to DLQ)
    public ControlRule rule; // set only on success

    public ControlRuleMessage() {}

    public static ControlRuleMessage good(ControlRule rule) {
        ControlRuleMessage m = new ControlRuleMessage();
        m.rule = rule;
        return m;
    }

    public static ControlRuleMessage bad(byte[] raw) {
        ControlRuleMessage m = new ControlRuleMessage();
        m.raw = raw;
        return m;
    }
}
