package local.home.arpa.telemetry.enrich;

import local.home.arpa.telemetry.serde.ContextEvent;
import local.home.arpa.telemetry.serde.ContextEventMessage;
import local.home.arpa.telemetry.serde.TelemetryMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;

/**
 * Joins sensor telemetry (input 1) against context events (input 2) on device id, using keyed
 * state - each physical device's "currently open cycle" is its own piece of state.
 *
 * Two pieces of per-device state are kept:
 *  - contextState: the CURRENTLY open cycle (cleared on TRACK_OUT, set on TRACK_IN). Drives
 *    per-tick enrichment of ordinary sensor readings.
 *  - lastTrackOutState: the timestamp of the most recent TRACK_OUT, NEVER cleared, only ever
 *    overwritten. This is what lets a quarantined "no open context" record distinguish "this
 *    device has never been tracked in yet" (null) from "it's between cycles, closed N ms ago"
 *    (set) - the two previously collapsed into the same indistinguishable signal.
 *
 * TRACK_OUT also emits a synthetic cycle-close TelemetryMessage (contextComplete=true, no
 * sensor fields, trackOut set) straight to the output, stamped at the TRACK_OUT event's own
 * timestamp - this is what gives IoTDB its own durable track_out record, rather than relying
 * on a sensor tick to happen to carry it (which it structurally can't, since track_out isn't
 * known until the cycle actually closes).
 *
 * Unrecognized event_type values are ignored, not rejected - keeps the context schema
 * forward-compatible with new event types later without a Flink redeploy being required just
 * to tolerate them.
 */
public class ContextEnrichmentFunction
        extends KeyedCoProcessFunction<String, TelemetryMessage, ContextEventMessage, TelemetryMessage> {

    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("machine-dlq-stream", TypeInformation.of(String.class));
    public static final OutputTag<String> INCOMPLETE_TRACK_TAG =
            new OutputTag<String>("incomplete-track-stream", TypeInformation.of(String.class));

    private transient ValueState<DeviceContext> contextState;
    private transient ValueState<Long> lastTrackOutState;
    private transient ObjectMapper json;

    @Override
    public void open(Configuration parameters) {
        contextState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("device-context", TypeInformation.of(DeviceContext.class)));
        lastTrackOutState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-track-out", Types.LONG));
        json = new ObjectMapper();
    }

    @Override
    public void processElement1(TelemetryMessage sensor, Context ctx, Collector<TelemetryMessage> out) throws Exception {
        if (sensor.raw != null) {
            out.collect(sensor); // bad decode - pass through; TelemetryRouter routes it to DLQ
            return;
        }

        DeviceContext dc = contextState.value();
        if (dc != null) {
            dc.dataPointCount++;
            contextState.update(dc); // ValueState mutations aren't guaranteed to persist without an explicit update()
            sensor.runId = dc.runId;
            sensor.partNo = dc.partNo;
            sensor.recipeName = dc.recipeName;
            sensor.trackIn = dc.trackIn;
            sensor.contextComplete = true;
            sensor.dataPointCount = dc.dataPointCount; // this reading's position within the cycle
        } else {
            sensor.contextComplete = false;
            sensor.lastKnownTrackOut = lastTrackOutState.value(); // null if never tracked at all
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
            DeviceContext existing = contextState.value();
            if (existing != null) {
                // A new cycle is opening while the previous one was never closed - its readings
                // already went to IoTDB tagged complete (they looked fine at the time) and will
                // never get a track_out. Surfaced here, same treatment as orphaned_track_out,
                // carrying enough of the abandoned cycle's identity for later reconciliation.
                try {
                    java.util.Map<String, Object> abandoned = new java.util.LinkedHashMap<>();
                    abandoned.put("device_id", e.deviceId);
                    abandoned.put("timestamp", e.eventTimestamp);
                    abandoned.put("reason", "abandoned_cycle");
                    abandoned.put("abandoned_run_id", existing.runId);
                    abandoned.put("abandoned_part_no", existing.partNo);
                    abandoned.put("abandoned_track_in", existing.trackIn);
                    abandoned.put("data_point_count_at_abandonment", existing.dataPointCount);
                    ctx.output(INCOMPLETE_TRACK_TAG, json.writeValueAsString(abandoned));
                } catch (Exception ignored) {
                    // formatting a small fixed-shape map should never fail; drop rather than risk the job
                }
            }

            DeviceContext dc = new DeviceContext();
            dc.runId = e.runId;
            dc.partNo = e.partNo;
            dc.recipeName = e.recipeName;
            dc.trackIn = e.eventTimestamp;
            contextState.update(dc);

            // Standalone IoTDB anchor row for the TRACK_IN itself - previously a track_in only
            // ever showed up indirectly, riding along on whatever sensor tick happened next.
            // A short or dropped-telemetry cycle could then have no precise track_in in IoTDB
            // at all. This row exists regardless of the abandoned-cycle check above.
            TelemetryMessage trackInMarker = new TelemetryMessage();
            trackInMarker.deviceId = e.deviceId;
            trackInMarker.timestamp = e.eventTimestamp;
            trackInMarker.contextComplete = true;
            trackInMarker.runId = e.runId;
            trackInMarker.partNo = e.partNo;
            trackInMarker.recipeName = e.recipeName;
            trackInMarker.trackIn = e.eventTimestamp;
            trackInMarker.contextEvent = "TRACK_IN";
            trackInMarker.dataPointCount = 0L;
            out.collect(trackInMarker);

        } else if ("TRACK_OUT".equals(e.eventType)) {
            DeviceContext dc = contextState.value();
            if (dc != null) {
                // Durable close marker, straight to IoTDB via the normal row-building path -
                // this is the record that makes track_out a first-class, queryable fact rather
                // than something inferred from a gap between readings.
                TelemetryMessage closeMarker = new TelemetryMessage();
                closeMarker.deviceId = e.deviceId;
                closeMarker.timestamp = e.eventTimestamp;
                closeMarker.contextComplete = true;
                closeMarker.runId = dc.runId;
                closeMarker.partNo = dc.partNo;
                closeMarker.recipeName = dc.recipeName;
                closeMarker.trackIn = dc.trackIn;
                closeMarker.trackOut = e.eventTimestamp;
                closeMarker.contextEvent = "TRACK_OUT_CLOSE";
                closeMarker.dataPointCount = dc.dataPointCount;
                out.collect(closeMarker);
            }
            if (dc == null) {
                // Still gets its own IoTDB row - sparse (no run_id/part_no/track_in, since none
                // are known), but the event itself genuinely happened at this timestamp and
                // that fact belongs in the source of truth, not just the quarantine topic.
                TelemetryMessage orphanMarker = new TelemetryMessage();
                orphanMarker.deviceId = e.deviceId;
                orphanMarker.timestamp = e.eventTimestamp;
                orphanMarker.contextComplete = true;
                orphanMarker.trackOut = e.eventTimestamp;
                orphanMarker.contextEvent = "TRACK_OUT_ORPHANED";
                out.collect(orphanMarker);

                // Stray/duplicate TRACK_OUT with nothing open to close - e.g. a job restart lost
                // the matching TRACK_IN's state, or a scanner double-fired. Previously silently
                // dropped; now surfaced on the same quarantine topic as incomplete telemetry so
                // it isn't invisible, with its own reason so it's not confused with a sensor-side gap.
                try {
                    java.util.Map<String, Object> orphan = new java.util.LinkedHashMap<>();
                    orphan.put("device_id", e.deviceId);
                    orphan.put("timestamp", e.eventTimestamp);
                    orphan.put("reason", "orphaned_track_out");
                    ctx.output(INCOMPLETE_TRACK_TAG, json.writeValueAsString(orphan));
                } catch (Exception ignored) {
                    // formatting a small fixed-shape map should never fail; drop rather than risk the job
                }
            }
            // The close time is still recorded even when orphaned - it's still real-world
            // evidence of when this device was last idle, useful for the next reading's gap calc.
            lastTrackOutState.update(e.eventTimestamp);
            contextState.clear();
        }
        // any other event_type: ignored (forward-compatible, not an error)
    }
}
