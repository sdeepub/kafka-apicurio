package local.home.arpa.telemetry.serde;

import java.util.HashMap;
import java.util.Map;

/**
 * Control limits for a MACHINE TYPE (e.g. "Press-A"), not an individual device - one rule
 * record covers every device of that type. upperLimits/lowerLimits are keyed by MEASUREMENT
 * NAME, built by stripping "max_"/"min_" off whatever numeric fields exist on the control
 * record, so a newly added "min_x"/"max_x" field starts producing limits automatically.
 */
public class ControlRule {
    public String machineType;
    public String version;
    public String ruleProfile;
    public Map<String, Double> upperLimits = new HashMap<>(); // measurement -> max
    public Map<String, Double> lowerLimits = new HashMap<>(); // measurement -> min

    public ControlRule() {}
}
