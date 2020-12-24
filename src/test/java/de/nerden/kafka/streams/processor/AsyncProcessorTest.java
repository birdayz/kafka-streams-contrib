package de.nerden.kafka.streams.processor;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.util.concurrent.MoreExecutors;
import de.nerden.kafka.streams.serde.KeyValueSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AsyncProcessorTest {

  TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;

  @BeforeEach
  public void setUp() {
    Topology topology = new Topology();
    topology.addSource(
        "sourceProcessor",
        Serdes.String().deserializer(),
        Serdes.String().deserializer(),
        "input-topic");
    topology.addProcessor(
        "processor",
        () ->
            new AsyncProcessor<String, String, Void, Void>(
                kv -> {
                  throw new RuntimeException();
                },
                MoreExecutors::newDirectExecutorService),
        "sourceProcessor");

    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("async-inflight"),
            Serdes.Long(),
            new KeyValueSerde<>(Serdes.String(), Serdes.String())),
        "processor");

    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("async-failed"),
            Serdes.Long(),
            new KeyValueSerde<>(Serdes.String(), Serdes.String())),
        "processor");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    testDriver = new TopologyTestDriver(topology, props);

    inputTopic =
        testDriver.createInputTopic(
            "input-topic", Serdes.String().serializer(), Serdes.String().serializer());
  }

  @Test
  @DisplayName("Test Process")
  public void testProcess() {
    String key = "test-key";
    String val = "test-value ";
    inputTopic.pipeInput(key, val);

    final KeyValueStore<Long, KeyValue<String, String>> keyValueStore =
        testDriver.getKeyValueStore("async-failed");
    final KeyValue<String, String> failed = keyValueStore.get(0L);

    assertThat(failed).isNotNull();
    assertThat(failed.key).isEqualTo(key);
    assertThat(failed.value).isEqualTo(val);
  }
}
