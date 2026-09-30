package local.home.arpa.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import local.home.arpa.telemetry.serde.ControlRule;
import local.home.arpa.telemetry.serde.ControlRuleDeserializer;
import local.home.arpa.telemetry.serde.ControlRuleMessage;
import local.home.arpa.telemetry.serde.SafeDeserializer;
import local.home.arpa.telemetry.serde.TelemetryMessage;
import local.home.arpa.telemetry.spc.RollingWindow;
import local.home.arpa.telemetry.spc.SpcEvaluator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iotdb.flink.DefaultIoTSerializationSchema;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class TelemetryPipelineJob {

    private static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("machine-dlq-stream", TypeInformation.of(String.class));
    private static final OutputTag<String> ALERT_TAG =
            new OutputTag<String>("machine-alert-stream", TypeInformation.of(String.class));
    private static final OutputTag<String> INCOMPLETE_TRACK_TAG =
            new OutputTag<String>("incomplete-track-stream", TypeInformation.of(String.class));

    private static final MapStateDescriptor<String, ControlRule> RULES_STATE =
            new MapStateDescriptor<>("control-rules", Types.STRING, TypeInformation.of(ControlRule.class));

    // Naming convention for deriving a machine TYPE from a device id: strip a trailing "-NN"
    // numeric suffix. "Press-01" -> "Press"; "Press-A-01" -> "Press-A"; "Press-B-03" -> "Press-B".
    // If your device naming doesn't follow this pattern, update this regex.
    private static final Pattern TRAILING_NUMBER = Pattern.compile("-\\d+$");

    static String deriveMachineType(String deviceId) {
        return TRAILING_NUMBER.matcher(deviceId).replaceAll("");
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // --- Fault tolerance -------------------------------------------------
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setMinPauseBetweenCheckpoints(5_000);
        cc.setCheckpointTimeout(60_000);
        cc.setTolerableCheckpointFailureNumber(3);
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.seconds(1), Time.minutes(1), 2.0, Time.minutes(10), 0.1));

        String kafkaBootstrap = "kafka-broker:9094";
        String apicurioUrl = "http://apicurio-registry:8080/apis/ccompat/v7";
        String telemetryTopic = "machine-telemetry";
        String controlTopic = "machine-control";

        // --- Telemetry source --------------------------------------------------
        KafkaSource<TelemetryMessage> kafkaSource = KafkaSource.<TelemetryMessage>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(telemetryTopic)
                .setGroupId("flink-telemetry-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new SafeDeserializer(apicurioUrl, telemetryTopic))
                .build();

        DataStream<TelemetryMessage> telemetry =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Telemetry-Source")
                        .uid("kafka-telemetry-source");

        // Keyed by device: SPC state (rolling windows) is inherently per PHYSICAL machine -
        // two "identical" presses still wear and drift differently, which is the whole reason
        // SPC exists. Bad/unparseable messages (deviceId == null) get a fallback key; they're
        // routed to the DLQ immediately in processElement and never touch keyed state.
        KeyedStream<TelemetryMessage, String> keyedTelemetry =
                telemetry.keyBy(m -> m.deviceId != null ? m.deviceId : "__unrouted__");

        // --- Control-rules source (broadcast, keyed by MACHINE TYPE) ------------------
        // EARLIEST: on (re)start the job must load the full current rule set. Publish rules
        // keyed by machine_type and enable log compaction on this topic.
        KafkaSource<ControlRuleMessage> controlSource = KafkaSource.<ControlRuleMessage>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(controlTopic)
                .setGroupId("flink-control-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ControlRuleDeserializer(apicurioUrl, controlTopic))
                .build();

        DataStream<ControlRuleMessage> controlRaw =
                env.fromSource(controlSource, WatermarkStrategy.noWatermarks(), "Kafka-Control-Rules-Source")
                        .uid("kafka-control-rules-source");

        BroadcastStream<ControlRule> controlBroadcast = controlRaw
                .filter(m -> m.rule != null) // malformed rule messages dropped, not sent to DLQ (low-volume config data)
                .map(m -> m.rule)
                .returns(TypeInformation.of(ControlRule.class))
                .broadcast(RULES_STATE);

        // --- Router: IoTDB rows (main) + DLQ + limit/SPC alerts + incomplete-track (side outputs) ---
        SingleOutputStreamOperator<Map<String, String>> processed = keyedTelemetry
                .connect(controlBroadcast)
                .process(new TelemetryRouter())
                .uid("telemetry-router");

        // --- Sink 1: DLQ ---
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-DLQ")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        processed.getSideOutput(DLQ_TAG).sinkTo(dlqSink).name("Dead-Letter-Queue-Sink").uid("dlq-sink");

        // --- Sink 2: Alerts (limit breaches + SPC rule violations, both as JSON strings) ---
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        processed.getSideOutput(ALERT_TAG).sinkTo(alertSink).name("Alert-Sink").uid("alert-sink");

        // --- Sink 3: Incomplete-track quarantine (for later analysis; not IoTDB, not checked) ---
        KafkaSink<String> incompleteTrackSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-incomplete-track")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        processed.getSideOutput(INCOMPLETE_TRACK_TAG).sinkTo(incompleteTrackSink)
                .name("Incomplete-Track-Sink").uid("incomplete-track-sink");

        // --- Sink 4: IoTDB ---
        IoTDBSinkOptions opts = new IoTDBSinkOptions();
        opts.setHost("iotdb");
        opts.setPort(6667);
        opts.setUser("root");
        opts.setPassword("root");
        opts.setTimeseriesOptionList(new ArrayList<>());

        IoTDBSink<Map<String, String>> iotdbSink =
                new IoTDBSink<>(opts, new DefaultIoTSerializationSchema());
        processed.addSink(iotdbSink).name("Apache-IoTDB-Warehouse-Sink").uid("iotdb-sink");

        env.execute("Flink-Industrial-Telemetry-Pipeline");
    }

    /**
     * Per device (keyed state) x per machine-type control limits (broadcast state):
     * - Bad/unparseable telemetry -> DLQ side output.
     * - Telemetry missing track_in or track_out -> incomplete-track side output ONLY
     *   (does not reach IoTDB, limit checks, or SPC yet - "for now, just quarantine").
     *   This only ever fires once track_in/track_out are made nullable in the schema;
     *   as required fields today, they can never actually be missing.
     * - Otherwise -> IoTDB row (main output), plus, for every measurement this device's
     *   machine-type rule defines a limit for: an engineering-limit check AND an SPC
     *   zone-rule check against that device's own rolling history.
     */
    private static class TelemetryRouter
            extends KeyedBroadcastProcessFunction<String, TelemetryMessage, ControlRule, Map<String, String>> {

        private transient ObjectMapper json;
        private transient MapState<String, RollingWindow> windowState;

        private static final MapStateDescriptor<String, RollingWindow> WINDOW_STATE =
                new MapStateDescriptor<>("spc-windows", Types.STRING, TypeInformation.of(RollingWindow.class));

        @Override
        public void open(Configuration parameters) {
            json = new ObjectMapper();
            windowState = getRuntimeContext().getMapState(WINDOW_STATE);
        }

        @Override
        public void processElement(TelemetryMessage m, ReadOnlyContext ctx, Collector<Map<String, String>> out)
                throws Exception {
            if (m.raw != null) {
                ctx.output(DLQ_TAG, new String(m.raw, StandardCharsets.UTF_8));
                return;
            }

            boolean incompleteTrack = !m.numericFields.containsKey("track_in")
                    || !m.numericFields.containsKey("track_out");
            if (incompleteTrack) {
                Map<String, Object> quarantined = new LinkedHashMap<>();
                quarantined.put("device_id", m.deviceId);
                quarantined.put("timestamp", m.timestamp);
                quarantined.put("has_track_in", m.numericFields.containsKey("track_in"));
                quarantined.put("has_track_out", m.numericFields.containsKey("track_out"));
                quarantined.put("fields", m.numericFields);
                ctx.output(INCOMPLETE_TRACK_TAG, json.writeValueAsString(quarantined));
                return;
            }

            out.collect(m.iotdbFields);

            ReadOnlyBroadcastState<String, ControlRule> rules = ctx.getBroadcastState(RULES_STATE);
            String machineType = deriveMachineType(m.deviceId);
            ControlRule rule = rules.get(machineType);
            if (rule == null) return; // no rule for this type yet - skip limit/SPC checks for now

            Set<String> measurements = new LinkedHashSet<>();
            measurements.addAll(rule.upperLimits.keySet());
            measurements.addAll(rule.lowerLimits.keySet());

            for (String measurement : measurements) {
                Double value = m.numericFields.get(measurement);
                if (value == null) continue; // this telemetry record doesn't carry that measurement

                checkLimits(ctx, m, rule, measurement, value);
                checkSpc(ctx, m, measurement, value);
            }
        }

        private void checkLimits(ReadOnlyContext ctx, TelemetryMessage m, ControlRule rule,
                                  String measurement, double value) {
            Double upper = rule.upperLimits.get(measurement);
            if (upper != null && value > upper) {
                emitAlert(ctx, m, "limit_breach", measurement, value,
                        "device_id", m.deviceId, "limit_type", "upper", "limit_value", upper,
                        "rule_profile", rule.ruleProfile, "rule_version", rule.version);
            }
            Double lower = rule.lowerLimits.get(measurement);
            if (lower != null && value < lower) {
                emitAlert(ctx, m, "limit_breach", measurement, value,
                        "device_id", m.deviceId, "limit_type", "lower", "limit_value", lower,
                        "rule_profile", rule.ruleProfile, "rule_version", rule.version);
            }
        }

        private void checkSpc(ReadOnlyContext ctx, TelemetryMessage m, String measurement, double value)
                throws Exception {
            RollingWindow window = windowState.get(measurement);
            if (window == null) window = new RollingWindow();
            window.push(value, SpcEvaluator.WINDOW_CAPACITY);
            windowState.put(measurement, window);

            for (SpcEvaluator.Violation v : SpcEvaluator.evaluate(window.values)) {
                emitAlert(ctx, m, "spc_rule", measurement, value,
                        "device_id", m.deviceId, "rule_number", v.rule, "rule_description", v.description);
            }
        }

        private void emitAlert(ReadOnlyContext ctx, TelemetryMessage m, String alertType,
                                String measurement, double value, Object... extra) {
            try {
                Map<String, Object> alert = new LinkedHashMap<>();
                alert.put("alert_type", alertType);
                alert.put("timestamp", m.timestamp);
                alert.put("measurement", measurement);
                alert.put("value", value);
                for (int i = 0; i + 1 < extra.length; i += 2) {
                    alert.put(String.valueOf(extra[i]), extra[i + 1]);
                }
                ctx.output(ALERT_TAG, json.writeValueAsString(alert));
            } catch (Exception e) {
                // Serialization of a small, fixed-shape map should never fail; if it does,
                // drop the alert rather than take down the job over an alert-formatting bug.
            }
        }

        @Override
        public void processBroadcastElement(ControlRule rule, Context ctx, Collector<Map<String, String>> out)
                throws Exception {
            BroadcastState<String, ControlRule> state = ctx.getBroadcastState(RULES_STATE);
            state.put(rule.machineType, rule);
        }
    }
}
