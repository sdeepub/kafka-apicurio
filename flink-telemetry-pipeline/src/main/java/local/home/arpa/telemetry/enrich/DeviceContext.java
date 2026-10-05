package local.home.arpa.telemetry.enrich;

/** The currently-open cycle for one device, held in keyed ValueState. Null/absent = no open
 *  cycle right now (machine idle, between parts, or context just hasn't arrived yet). */
public class DeviceContext {
    public String runId;
    public String partNo;
    public String recipeName;
    public Long trackIn;

    public DeviceContext() {}
}
