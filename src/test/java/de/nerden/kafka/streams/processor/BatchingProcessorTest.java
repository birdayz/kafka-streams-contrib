package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.BatchEntryKey;
import de.nerden.kafka.streams.serde.BatchEntryKeySerde;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchingProcessorTest {

  private TestInputTopic<String, String> inputTopic;
  private TopologyTestDriver testDriver;
  private Processor<String, String> processor;
  MockProcessorContext context;

  @Before
  public void setup() {
    processor = new BatchingProcessor<>();
    StoreBuilder<KeyValueStore<BatchEntryKey<String>, String>> store =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("batch"),
                new BatchEntryKeySerde<>(Serdes.String()),
                Serdes.String())
            .withLoggingDisabled();

    final KeyValueStore<BatchEntryKey<String>, String> s = store.build();

    context = new MockProcessorContext();

    s.init(context, s);
    context.register(s, null);
    processor.init(context);
  }

  @Test
  public void TestStuff() {
    context.setOffset(0L);
    processor.process("abc", "def");
    context.setOffset(1L);
    processor.process("abc", "def2");
    context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

    Iterator<CapturedForward> i = context.forwarded().iterator();
    Assert.assertTrue(i.hasNext());
    Assert.assertEquals(
        List.of(KeyValue.pair("abc", "def"), KeyValue.pair("abc", "def2")),
        i.next().keyValue().value);
  }
}
