package flightimporter;

import models.SkyOneAirlinesFlightData;
import models.FlightData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.io.InputStream;
import java.util.Properties;
import java.time.ZonedDateTime;

public class FlightImporterJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        Properties producerConfig = new Properties();
        try (InputStream stream = FlightImporterJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerConfig.load(stream);
            producerConfig.load(stream);
        }

        KafkaSource<SkyOneAirlinesFlightData> skyOneSource = KafkaSource.<SkyOneAirlinesFlightData>builder()
                .setProperties(consumerConfig)
                .setTopics("skyone")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(SkyOneAirlinesFlightData.class))
                .build();

        DataStream<SkyOneAirlinesFlightData> skyOneStream = env
                .fromSource(skyOneSource, WatermarkStrategy.noWatermarks(), "skyone_source");
        KafkaRecordSerializationSchema<FlightData> flightDataSerializer = KafkaRecordSerializationSchema
                .<FlightData>builder()
                .setTopic("flightdata")
                .setValueSerializationSchema(
                        new JsonSerializationSchema<FlightData>(
                                () -> {
                                    return new ObjectMapper().registerModule(new JavaTimeModule());
                                }))
                .build();

        KafkaSink<FlightData> flightDataSink = KafkaSink.<FlightData>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(flightDataSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        DataStream<FlightData> flightDataStream = defineWorkflow(skyOneStream);

        //flightDataStream.print(); // dump stuff to stdout
        flightDataStream
        .sinkTo(flightDataSink)
        .name("flightdatasink");


        env.execute("FlightImporter");
    }

    public static DataStream<FlightData> defineWorkflow(DataStream<SkyOneAirlinesFlightData> skyOneSource) {
        return skyOneSource
                .filter(s -> (s.getFlightArrivalTime()).isAfter(ZonedDateTime.now())) // only flights yet to arrive
                .map(s -> s.toFlightData()); // convert to generic format
    }
}