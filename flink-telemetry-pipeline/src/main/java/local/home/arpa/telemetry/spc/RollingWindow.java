package local.home.arpa.telemetry.spc;

import java.util.ArrayList;

/** A capped FIFO buffer of recent readings for one (device, measurement) series. Flink-checkpointed keyed state. */
public class RollingWindow {
    public ArrayList<Double> values = new ArrayList<>();

    public RollingWindow() {}

    public void push(double v, int capacity) {
        values.add(v);
        while (values.size() > capacity) {
            values.remove(0);
        }
    }
}
