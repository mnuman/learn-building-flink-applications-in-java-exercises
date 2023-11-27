package flightimporter;

import models.SkyOneAirlinesFlightData;
import models.SunsetAirFlightData;
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
        }
        try (InputStream stream = FlightImporterJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
            producerConfig.load(stream);
        }
        // first source: SkyOne
        KafkaSource<SkyOneAirlinesFlightData> skyOneSource = KafkaSource.<SkyOneAirlinesFlightData>builder()
                .setProperties(consumerConfig)
                .setTopics("skyone")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(SkyOneAirlinesFlightData.class))
                .build();
        // second source: SunsetAir
        KafkaSource<SunsetAirFlightData> sunsetSource = KafkaSource.<SunsetAirFlightData>builder()
                .setProperties(consumerConfig)
                .setTopics("sunset")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(SunsetAirFlightData.class))
                .build();

        DataStream<SkyOneAirlinesFlightData> skyOneStream = env
                .fromSource(skyOneSource, WatermarkStrategy.noWatermarks(), "skyone_source");
        DataStream<SunsetAirFlightData> sunsetStream = env
                .fromSource(sunsetSource, WatermarkStrategy.noWatermarks(), "sunset_source");

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
                .build();

        DataStream<FlightData> flightDataStream = defineWorkflow(skyOneStream, sunsetStream);

        //flightDataStream.print(); // dump stuff to stdout
        flightDataStream
        .sinkTo(flightDataSink)
        .name("flightdatasink");


        env.execute("FlightImporter");
    }

    public static DataStream<FlightData> defineWorkflow(
        DataStream<SkyOneAirlinesFlightData> stream1,
        DataStream<SunsetAirFlightData> stream2
    ) {
        // both sources have payloads that implement getFlightArrivalTime() and toFlightData() operations
        DataStream<FlightData> s1 = stream1
            .filter(s -> (s.getFlightArrivalTime()).isAfter(ZonedDateTime.now())) // only flights yet to arrive
            .map(s -> s.toFlightData()); // convert to generic format
        DataStream<FlightData> s2 = stream2
            .filter(s -> (s.getArrivalTime()).isAfter(ZonedDateTime.now())) // only flights yet to arrive
            .map(s -> s.toFlightData()); // convert to generic format
        return s1.union(s2);
    }
}