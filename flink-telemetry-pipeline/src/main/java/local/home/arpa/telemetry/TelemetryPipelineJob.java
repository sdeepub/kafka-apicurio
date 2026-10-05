package local.home.arpa.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import local.home.arpa.telemetry.enrich.ContextEnrichmentFunction;
import local.home.arpa.telemetry.serde.ContextEventDeserializer;
import local.home.arpa.telemetry.serde.ContextEventMessage;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class TelemetryPipelineJob {

    private static final OutputTag<String> DLQ_TAG = ContextEnrichmentFunction.DLQ_TAG; // shared across operators
    private static final OutputTag<String> ALERT_TAG =
            new OutputTag<String>("machine-alert-stream", TypeInformation.of(String.class));
    private static final OutputTag<String> INCOMPLETE_TRACK_TAG =
            new OutputTag<String>("incomplete-track-stream", TypeInformation.of(String.class));

    private static final MapStateDescriptor<String, ControlRule> RULES_STATE =
            new MapStateDescriptor<>("control-rules", Types.STRING, TypeInformation.of(ControlRule.class));

    // Naming convention for deriving a machine TYPE from a device id: strip a trailing "-NN"
    // numeric suffix. "Press-A-01" -> "Press-A"; "CNC-07" -> "CNC". Used both for control-rule
    // lookup AND for the IoTDB device path (see buildIotdbRow) - fixes a prior bug where the
    // IoTDB path had "press" hardcoded into it regardless of actual machine type.
    private static final Pattern TRAILING_NUMBER = Pattern.compile("-\\d+$");

    static String deriveMachineType(String deviceId) {
        return TRAILING_NUMBER.matcher(deviceId).replaceAll("");
    }

    private static String sanitize(String s) {
        return s.replace("-", "_");
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
        String controlTopic = "machine-control";
        String contextTopic = "machine-context-events";

        // --- Telemetry sources: ONE topic per machine family, unioned ----------------------
        // To onboard a new machine family (e.g. CNC mills): register its own Avro schema in
        // Apicurio, create its own Kafka topic, and add that topic name to this list. Nothing
        // else in this file needs to change - SafeDeserializer resolves each topic's own
        // schema independently via the registry, and the resulting streams are unioned below
        // into the same enrichment/routing pipeline every other family already goes through.
        List<String> telemetryTopics = List.of("machine-telemetry");

        DataStream<TelemetryMessage> telemetryUnioned = null;
        for (String topic : telemetryTopics) {
            KafkaSource<TelemetryMessage> src = KafkaSource.<TelemetryMessage>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setTopics(topic)
                    .setGroupId("flink-telemetry-consumer-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setDeserializer(new SafeDeserializer(apicurioUrl, topic))
                    .build();
            DataStream<TelemetryMessage> s =
                    env.fromSource(src, WatermarkStrategy.noWatermarks(), "Kafka-Telemetry-Source-" + topic)
                            .uid("kafka-telemetry-source-" + topic);
            telemetryUnioned = (telemetryUnioned == null) ? s : telemetryUnioned.union(s);
        }

        // --- Context-events source (keyed join input, NOT broadcast - see ContextEnrichmentFunction) ---
        // EARLIEST: a job started fresh (no savepoint restore) must be able to recover a cycle
        // that was already open (TRACK_IN already published) before the job started. Bounded
        // by this topic's own retention - NOT compacted (it's an event log, unlike
        // machine-control which holds current-state-per-key and IS compacted).
        KafkaSource<ContextEventMessage> contextSource = KafkaSource.<ContextEventMessage>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(contextTopic)
                .setGroupId("flink-context-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ContextEventDeserializer(apicurioUrl, contextTopic))
                .build();
        DataStream<ContextEventMessage> contextRaw =
                env.fromSource(contextSource, WatermarkStrategy.noWatermarks(), "Kafka-Context-Source")
                        .uid("kafka-context-source");

        // --- Enrichment: keyed-state join of telemetry against context, per device ---
        KeyedStream<TelemetryMessage, String> keyedTelemetry =
                telemetryUnioned.keyBy(m -> m.deviceId != null ? m.deviceId : "__unrouted__");
        KeyedStream<ContextEventMessage, String> keyedContext =
                contextRaw.keyBy(m -> m.event != null ? m.event.deviceId : "__unrouted__");

        SingleOutputStreamOperator<TelemetryMessage> enriched = keyedTelemetry
                .connect(keyedContext)
                .process(new ContextEnrichmentFunction())
                .uid("context-enrichment");

        // --- Control-rules source (broadcast, keyed by MACHINE TYPE) ------------------
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
                .filter(m -> m.rule != null)
                .map(m -> m.rule)
                .returns(TypeInformation.of(ControlRule.class))
                .broadcast(RULES_STATE);

        // --- Router: IoTDB rows (main) + DLQ + alerts + incomplete-track (side outputs) ---
        SingleOutputStreamOperator<Map<String, String>> processed = enriched
                .keyBy(m -> m.deviceId != null ? m.deviceId : "__unrouted__")
                .connect(controlBroadcast)
                .process(new TelemetryRouter())
                .uid("telemetry-router");

        // --- Sink 1: DLQ (fed from BOTH the enrichment step and the router) ---
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-DLQ")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        enriched.getSideOutput(DLQ_TAG)
                .union(processed.getSideOutput(DLQ_TAG))
                .sinkTo(dlqSink).name("Dead-Letter-Queue-Sink").uid("dlq-sink");

        // --- Sink 2: Alerts (limit breaches + SPC rule violations) ---
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        processed.getSideOutput(ALERT_TAG).sinkTo(alertSink).name("Alert-Sink").uid("alert-sink");

        // --- Sink 3: Incomplete-track flag copy (not exclusive - record still reaches IoTDB) ---
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
     * Combines sensor fields with context fields into the final IoTDB row, and runs limit/SPC
     * checks. Incomplete-track records are flagged (quarantine side output) but NOT excluded
     * from IoTDB or from limit/SPC checks - see the earlier design discussion: sensor readings
     * are valid whether or not a part cycle is currently known for them.
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

            if (!m.contextComplete) {
                Map<String, Object> quarantined = new LinkedHashMap<>();
                quarantined.put("device_id", m.deviceId);
                quarantined.put("timestamp", m.timestamp);
                quarantined.put("reason", "no_open_context");
                ctx.output(INCOMPLETE_TRACK_TAG, json.writeValueAsString(quarantined));
                // falls through - still written to IoTDB, still checked below
            }

            out.collect(buildIotdbRow(m));

            ReadOnlyBroadcastState<String, ControlRule> rules = ctx.getBroadcastState(RULES_STATE);
            String machineType = deriveMachineType(m.deviceId);
            ControlRule rule = rules.get(machineType);
            if (rule == null) return;

            Set<String> measurements = new LinkedHashSet<>();
            measurements.addAll(rule.upperLimits.keySet());
            measurements.addAll(rule.lowerLimits.keySet());

            for (String measurement : measurements) {
                Double value = m.numericFields.get(measurement);
                if (value == null) continue;

                checkLimits(ctx, m, rule, measurement, value);
                checkSpc(ctx, m, measurement, value);
            }
        }

        private Map<String, String> buildIotdbRow(TelemetryMessage m) {
            String machineType = deriveMachineType(m.deviceId);
            String devicePath = "root.local.home.arpa." + sanitize(machineType) + "." + sanitize(m.deviceId);

            StringBuilder names = new StringBuilder();
            StringBuilder types = new StringBuilder();
            StringBuilder values = new StringBuilder();

            for (Map.Entry<String, String> e : m.sensorValues.entrySet()) {
                append(names, types, values, e.getKey(), m.sensorTypes.get(e.getKey()), e.getValue());
            }
            if (m.contextComplete) {
                if (m.runId != null) append(names, types, values, "run_id", "TEXT", m.runId);
                if (m.partNo != null) append(names, types, values, "part_no", "TEXT", m.partNo);
                if (m.recipeName != null) append(names, types, values, "recipe_name", "TEXT", m.recipeName);
                if (m.trackIn != null) append(names, types, values, "track_in", "INT64", String.valueOf(m.trackIn));
            }

            Map<String, String> row = new LinkedHashMap<>();
            row.put("device", devicePath);
            row.put("timestamp", String.valueOf(m.timestamp));
            row.put("measurements", names.toString());
            row.put("types", types.toString());
            row.put("values", values.toString());
            return row;
        }

        private void append(StringBuilder names, StringBuilder types, StringBuilder values,
                             String name, String type, String value) {
            if (names.length() > 0) { names.append(','); types.append(','); values.append(','); }
            names.append(name);
            types.append(type);
            values.append(value.replace(',', ';'));
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
                // Serialization of a small, fixed-shape map should never fail; drop rather than
                // take down the job over an alert-formatting bug.
            }
        }

        @Override
        public void processBroadcastElement(ControlRule rule, Context ctx, Collector<Map<String, String>> out) {
            BroadcastState<String, ControlRule> state;
            try {
                state = ctx.getBroadcastState(RULES_STATE);
                state.put(rule.machineType, rule);
            } catch (Exception ignored) {
                // broadcast state access failing here would indicate a serialization/config bug,
                // not a data problem - nothing sensible to do but drop this rule update
            }
        }
    }
}
