package local.home.arpa.telemetry.enrich;

import local.home.arpa.telemetry.serde.ContextEvent;
import local.home.arpa.telemetry.serde.ContextEventMessage;
import local.home.arpa.telemetry.serde.TelemetryMessage;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;

/**
 * Joins sensor telemetry (input 1) against context events (input 2) on device id, using keyed
 * state rather than broadcast state - each physical device's "currently open cycle" is its own
 * piece of state, not shared.
 *
 * TRACK_IN opens a cycle (stores run_id/part_no/recipe_name/track_in); TRACK_OUT closes it
 * (clears state). Every sensor reading gets enriched with whatever is CURRENTLY open at the
 * moment it's processed - unlike the old design, a reading no longer needs both track_in AND
 * track_out present in the same message; every tick during an active cycle gets the context.
 * A reading between TRACK_OUT and the next TRACK_IN has genuinely no open cycle - that's
 * expected, normal operation, not an error (contextComplete = false; the router decides what
 * to do with that).
 *
 * Unrecognized event_type values are ignored here, not rejected - this is what keeps the
 * context schema forward-compatible with new event types later (e.g. "RUN_PAUSE") without a
 * Flink redeploy being required just to tolerate them.
 */
public class ContextEnrichmentFunction
        extends KeyedCoProcessFunction<String, TelemetryMessage, ContextEventMessage, TelemetryMessage> {

    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("machine-dlq-stream", TypeInformation.of(String.class));

    private transient ValueState<DeviceContext> contextState;

    @Override
    public void open(Configuration parameters) {
        contextState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("device-context", TypeInformation.of(DeviceContext.class)));
    }

    @Override
    public void processElement1(TelemetryMessage sensor, Context ctx, Collector<TelemetryMessage> out) throws Exception {
        if (sensor.raw != null) {
            out.collect(sensor); // bad decode - pass through; TelemetryRouter routes it to DLQ
            return;
        }

        DeviceContext dc = contextState.value();
        if (dc != null) {
            sensor.runId = dc.runId;
            sensor.partNo = dc.partNo;
            sensor.recipeName = dc.recipeName;
            sensor.trackIn = dc.trackIn;
            sensor.contextComplete = true;
        } else {
            sensor.contextComplete = false;
        }
        out.collect(sensor);
    }

    @Override
    public void processElement2(ContextEventMessage msg, Context ctx, Collector<TelemetryMessage> out) throws Exception {
        if (msg.event == null) {
            ctx.output(DLQ_TAG, new String(msg.raw, StandardCharsets.UTF_8));
            return;
        }

        ContextEvent e = msg.event;
        if ("TRACK_IN".equals(e.eventType)) {
            DeviceContext dc = new DeviceContext();
            dc.runId = e.runId;
            dc.partNo = e.partNo;
            dc.recipeName = e.recipeName;
            dc.trackIn = e.eventTimestamp;
            contextState.update(dc);
        } else if ("TRACK_OUT".equals(e.eventType)) {
            contextState.clear();
        }
        // any other event_type: ignored (forward-compatible, not an error)
    }
}
