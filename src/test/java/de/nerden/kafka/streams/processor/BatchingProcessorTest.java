package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.MoreTransformers;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchingProcessorTest {

  private TestInputTopic<String, String> inputTopic;
  private TopologyTestDriver testDriver;
  private Transformer<String, String, KeyValue<String, List<String>>> processor;
  MockProcessorContext context;

  @Before
  public void setup() {
    final TransformerSupplier<String, String, KeyValue<String, List<String>>> batch =
        MoreTransformers.Batch(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("batch")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

    context = new MockProcessorContext();

    batch
        .stores()
        .forEach(
            storeBuilder -> {
              final StateStore store = storeBuilder.build();
              store.init(context, store);
              context.register(store, null);
            });
    processor = batch.get();
    processor.init(context);
  }

  @Test
  public void TestStuff() {
    context.setOffset(0L);
    processor.transform("abc", "def");
    context.setOffset(1L);
    processor.transform("abc", "def2");
    context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

    Iterator<CapturedForward> i = context.forwarded().iterator();
    Assert.assertTrue(i.hasNext());
    Assert.assertEquals(
        List.of(KeyValue.pair("abc", "def"), KeyValue.pair("abc", "def2")),
        i.next().keyValue().value);
  }
}
