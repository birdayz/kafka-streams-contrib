package de.nerden.kafka.streams.processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class BatchingProcessor<K, V> implements Processor<K, V> {

  private KeyValueStore<Long, KeyValue<K, V>> store;
  private ProcessorContext context;

  private long maxBatchSize;

  private long currentBatchSize;

  public BatchingProcessor(long maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    store = (KeyValueStore<Long, KeyValue<K, V>>) context.getStateStore("batch");

    final KeyValueIterator<Long, KeyValue<K, V>> all = this.store.all();
    all.forEachRemaining(
        longKeyValueKeyValue -> {
          this.currentBatchSize++;
        });
    all.close();

    this.context = context;
    this.context.schedule(
        Duration.ofMillis(10000),
        PunctuationType.WALL_CLOCK_TIME,
        timestamp -> {
          forwardBatch();
        });
  }

  @Override
  public void process(final K key, final V value) {
    this.store.put(context.offset(), KeyValue.pair(key, value));

    if (this.currentBatchSize >= this.maxBatchSize) {
      forwardBatch();
    }
  }

  private void forwardBatch() {
    final KeyValueIterator<Long, KeyValue<K, V>> all = this.store.all();

    List<KeyValue<Long, KeyValue<K, V>>> sorted = new ArrayList<>();

    all.forEachRemaining(sorted::add);
    all.close();

    // Sort by offset, because iterating on the store does not guarantee ordering
    sorted.sort(Comparator.comparing(o -> o.key));

    List<KeyValue<K, V>> batch =
        sorted.stream().map(item -> item.value).collect(Collectors.toList());

    if (!batch.isEmpty()) {
      context.forward(null, batch);
    }

    KeyValueIterator<Long, KeyValue<K, V>> i = store.all();

    i.forEachRemaining(
        longKeyValueKeyValue -> {
          store.delete(longKeyValueKeyValue.key);
        });

    i.close();
  }

  @Override
  public void close() {}
}
