package de.nerden.kafka.streams.processor;

import com.google.common.truth.Truth;
import de.nerden.kafka.streams.serde.AsyncMessageSerde;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AsyncTransformerTest {

  TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

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
                              return kv;
                            }),
                    failed -> false,
                    "inflight",
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
  @DisplayName("Check if async result is forwarded")
  void TestSimple() {
    inputTopic.pipeInput("key-a", "value-a");

    inputTopic.pipeInput("key-b", "value-b"); // Should block until previous KV is processed
    testDriver.advanceWallClockTime(Duration.ofMillis(1000));

    KeyValue<String, String> kv = outputTopic.readKeyValue();
    Truth.assertThat(kv).isEqualTo(KeyValue.pair("key-a", "value-a"));
  }
}
