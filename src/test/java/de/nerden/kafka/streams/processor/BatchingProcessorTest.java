package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.MoreTransformers;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BatchingProcessorTest {

  private Transformer<String, String, KeyValue<String, List<String>>> processor;
  MockProcessorContext context;

  @BeforeEach
  public void setup() {
    final TransformerSupplier<String, String, KeyValue<String, List<String>>> batch =
        MoreTransformers.Batch(
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("batch")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withLoggingDisabled());

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
  public void TestSunshineCase() {
    context.setOffset(0L);
    processor.transform("abc", "def");
    context.setOffset(1L);
    processor.transform("abc", "def2");
    context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

    Iterator<CapturedForward> i = context.forwarded().iterator();
    Assertions.assertTrue(i.hasNext());

    KeyValue<String, List<String>> firstBatch = i.next().keyValue();

    Assertions.assertEquals(List.of("def", "def2"), firstBatch.value);
    Assertions.assertEquals("abc", firstBatch.key);
  }
}
