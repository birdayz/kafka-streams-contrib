package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.BatchEntryKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class BatchingTransformer<K, V> implements Transformer<K, V, KeyValue<K, List<V>>> {

  private KeyValueStore<BatchEntryKey<K>, V> store;
  private ProcessorContext context;

  private Map<K, Long> entries;

  private String storeName;

  Cancellable punctuation;

  public BatchingTransformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    store = (KeyValueStore<BatchEntryKey<K>, V>) context.getStateStore(storeName);
    entries = new HashMap<>();

    try (KeyValueIterator<BatchEntryKey<K>, V> all = this.store.all()) {
      all.forEachRemaining(item -> this.entries.merge(item.key.getKey(), 1L, Long::sum));
    }

    this.context = context;
    this.punctuation =
        this.context.schedule(
            Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, timestamp -> forwardBatch());
  }

  @Override
  public KeyValue<K, List<V>> transform(K key, V value) {
    this.store.put(new BatchEntryKey<>(key, this.context.offset()), value);
    this.entries.merge(key, 1L, Long::sum);
    return null;
  }

  private void forwardBatch() {
    this.entries.forEach(
        (key, offset) -> {
          List<KeyValue<K, V>> batch = new ArrayList<>();
          try (KeyValueIterator<BatchEntryKey<K>, V> range =
              this.store.range(
                  new BatchEntryKey<>(key, 0L), new BatchEntryKey<>(key, Long.MAX_VALUE))) {

            range.forEachRemaining(
                item -> {
                  batch.add(KeyValue.pair(item.key.getKey(), item.value));

                  // No idea if this allowed while we're in the iterator
                  this.store.delete(item.key);
                });
          }
          this.context.forward(key, batch);
        });

    this.entries.clear();
  }

  @Override
  public void close() {
    this.store = null;
    this.entries = null;
    this.punctuation.cancel();
    this.punctuation = null;
  }
}