package de.nerden.kafka.streams.processor.examples;

import de.nerden.kafka.streams.MoreTransformers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class BatchingProcessorExample {

  private static final String KAFKA_BROKERS = "localhost:9092";
  private static final String APPLICATION_ID = "testBatch";

  public static void main(String[] args) {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> data =
        builder.stream("data", Consumed.with(Serdes.String(), Serdes.String()));

    // Alternative: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("batch")

    data.transform(
            MoreTransformers.Batch(
                Materialized.<String, String>as(Stores.persistentKeyValueStore("batch"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())))
        .peek(
            (key, value) -> {
              System.out.println(value);
            });

    KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
    streams.start();
  }

  private static Properties getProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0L);
    props.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndFailExceptionHandler.class.getName());
    return props;
  }
}
