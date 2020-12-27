package de.nerden.kafka.streams.processor.examples;

import de.nerden.kafka.streams.processor.AsyncProcessorSupplier;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

public class AsyncProcessorExample {

  private static final String KAFKA_BROKERS = "localhost:9092";
  private static final String APPLICATION_ID = "test";

  public static void main(String[] args) {
    Topology t = new Topology();

    t.addSource("data", Serdes.String().deserializer(), Serdes.String().deserializer(), "data");
    t.addProcessor(
        "async",
        new AsyncProcessorSupplier<>(
            "inflight",
            "failed",
            Serdes.String(),
            Serdes.String(),
            kv -> {
              throw new RuntimeException("fail");
            }),
        "data");

    KafkaStreams streams = new KafkaStreams(t, getProperties());
    streams.start();
  }

  private static Properties getProperties() {
    Properties props = new Properties();
    props.put("request.timeout.ms", 60000);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0L);
    props.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndFailExceptionHandler.class.getName());
    return props;
  }
}
