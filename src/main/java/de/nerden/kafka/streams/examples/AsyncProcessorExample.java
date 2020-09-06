package de.nerden.kafka.streams.examples;

import de.nerden.kafka.streams.processor.AsyncProcessor;
import de.nerden.kafka.streams.processor.AsyncProcessorSupplier;
import de.nerden.kafka.streams.serde.KeyValueSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class AsyncProcessorExample {

  private static final String KAFKA_BROKERS = "localhost:9092";
  private static final String APPLICATION_ID = "test";

  public static void main(String[] args) {
    Topology t = new Topology();

    t.addSource("data", Serdes.String().deserializer(), Serdes.String().deserializer(), "data");
    t.addProcessor(
        "async",
            new AsyncProcessorSupplier<>(
                    "data-backlog", Serdes.String(), Serdes.String(), System.out::println),
        "data");

    KafkaStreams streams = new KafkaStreams(t, getProperties());
    streams.start();
  }

  private static Properties getProperties() {
    Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.setProperty(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024L); // 100 MB
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    props.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndFailExceptionHandler.class.getName());
    return props;
  }
}
