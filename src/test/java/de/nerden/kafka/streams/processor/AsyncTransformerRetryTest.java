package de.nerden.kafka.streams.processor;

import com.google.common.truth.Truth;
import de.nerden.kafka.streams.AsyncMessage;
import de.nerden.kafka.streams.serde.AsyncMessageSerde;
import de.nerden.kafka.streams.serde.KeyValueSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

class AsyncTransformerRetryTest {

  TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private int retryNo = 0;

  @BeforeEach
  public void setUp() {
    StreamsBuilder bldr = new StreamsBuilder();
    bldr.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .transform(
            () ->
                new AsyncTransformer<String, String>(
                    kv ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              try {
                                Thread.sleep(500);
                              } catch (InterruptedException e) {
                                e.printStackTrace();
                              }
                              if (retryNo > 0) {
                                return kv;
                              }
                              retryNo++;
                              throw new RuntimeException("random network fail");
                            }),
                    (retryMessage, e) -> {
                      return true;
                    },
                    "inflight",
                    "failed",
                    1,
                    5000),
            Named.as("async-transform"))
        .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

    Topology topology = bldr.build();

    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("inflight"),
            Serdes.Long(),
            new AsyncMessageSerde<>(Serdes.String(), Serdes.String())),
        "async-transform");

    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("failed"),
            Serdes.Long(),
            new AsyncMessageSerde<>(Serdes.String(), Serdes.String())),
        "async-transform");

    testDriver = new TopologyTestDriver(topology, new Properties());

    inputTopic =
        testDriver.createInputTopic(
            "input-topic", Serdes.String().serializer(), Serdes.String().serializer());

    outputTopic =
        testDriver.createOutputTopic(
            "output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  @Test
  @DisplayName("Check if record is forwarded after successfully retrying")
  void testRetryOnce() throws InterruptedException {
    inputTopic.pipeInput("key-a", "value-a");

    List<KeyValue<String, String>> keyValues;

    int i = 0;
    while (true) {
      i++;
      if (!outputTopic.isEmpty()) {
        keyValues = outputTopic.readKeyValuesToList();
        break;
      }

      Thread.sleep(100);
      testDriver.advanceWallClockTime(Duration.ofMillis(100));

      if (i > 30) {
        Assertions.fail();
      }
    }

    Truth.assertThat(keyValues).containsExactly(KeyValue.pair("key-a", "value-a"));
  }
}
