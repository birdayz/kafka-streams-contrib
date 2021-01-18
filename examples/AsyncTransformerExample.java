package de.nerden.kafka.streams.processor.examples;

import de.nerden.kafka.streams.MoreTransformers;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Use github.com/birdayz/kaf to test. Set up: "kaf topic create data -p 4" Test/create 100 input
 * messages: "echo -n 'record' | kaf produce data --partitioner jvm --key def -n 100"
 */
public class AsyncTransformerExample {

  private static final String KAFKA_BROKERS = "localhost:9092";
  private static final String APPLICATION_ID = "test";

  public static void main(String[] args) {
    ExecutorService es = Executors.newFixedThreadPool(10);

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> data =
        builder.stream("data", Consumed.with(Serdes.String(), Serdes.String()));

    data.transform(
            MoreTransformers.Async(
                Materialized.with(Serdes.String(), Serdes.String()),
                msg ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          try {
                            Thread.sleep(6000);
                          } catch (InterruptedException e) {
                            e.printStackTrace();
                          }
                          return msg;
                        },
                        es),
                retry -> false))
        .peek(
            (key, value) -> {
              System.out.printf("Received async-processed %s=%s\n", key, value);
            });

    KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
    streams.start();
  }

  private static Properties getProperties() {
    Properties props = new Properties();
    props.put("request.timeout.ms", 60000);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
    props.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndFailExceptionHandler.class.getName());
    return props;
  }
}
