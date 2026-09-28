package local.home.arpa.telemetry;

import local.home.arpa.telemetry.serde.SafeDeserializer;
import local.home.arpa.telemetry.serde.TelemetryMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iotdb.flink.DefaultIoTSerializationSchema;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

public class TelemetryPipelineJob {

    private static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("machine-dlq-stream", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String kafkaBootstrap = "kafka-broker:9094";
        String apicurioUrl = "http://apicurio-registry:8080/apis/ccompat/v7";

        KafkaSource<TelemetryMessage> kafkaSource = KafkaSource.<TelemetryMessage>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("machine-telemetry")
                .setGroupId("flink-telemetry-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new SafeDeserializer(apicurioUrl))
                .build();

        DataStream<TelemetryMessage> raw =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Telemetry-Source");

        // Router: good records -> main output, unparseable -> DLQ side output
        SingleOutputStreamOperator<Map<String, String>> processed = raw.process(
                new ProcessFunction<TelemetryMessage, Map<String, String>>() {
                    @Override
                    public void processElement(TelemetryMessage m, Context ctx, Collector<Map<String, String>> out) {
                        if (m.fields != null) {
                            out.collect(m.fields);
                        } else {
                            ctx.output(DLQ_TAG, new String(m.raw, StandardCharsets.UTF_8));
                        }
                    }
                });

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("machine-DLQ")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        processed.getSideOutput(DLQ_TAG).sinkTo(dlqSink).name("Dead-Letter-Queue-Sink");

        IoTDBSinkOptions opts = new IoTDBSinkOptions();
        opts.setHost("iotdb");
        opts.setPort(6667);
        opts.setUser("root");
        opts.setPassword("root");
        opts.setTimeseriesOptionList(new ArrayList<>()); // sink constructor iterates this; null => NPE

        IoTDBSink<Map<String, String>> iotdbSink =
                new IoTDBSink<>(opts, new DefaultIoTSerializationSchema());
        processed.addSink(iotdbSink).name("Apache-IoTDB-Warehouse-Sink");

        env.execute("Flink-Industrial-Telemetry-Pipeline");
    }
}
