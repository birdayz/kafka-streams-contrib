package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.BatchKey;
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

  private KeyValueStore<BatchKey<K>, V> store;
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
    store = (KeyValueStore<BatchKey<K>, V>) context.getStateStore(storeName);
    entries = new HashMap<>();

    try (KeyValueIterator<BatchKey<K>, V> all = this.store.all()) {
      all.forEachRemaining(item -> this.entries.merge(item.key.getKey(), 1L, Long::sum));
    }

    this.context = context;
    this.punctuation =
        this.context.schedule(
            Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, timestamp -> forwardBatch());
  }

  @Override
  public KeyValue<K, List<V>> transform(K key, V value) {
    this.store.put(new BatchKey<>(key, this.context.offset()), value);
    this.entries.merge(key, 1L, Long::sum);
    return null;
  }

  private void forwardBatch() {
    List<BatchKey<K>> itemsToDelete = new ArrayList<>();
    this.entries.forEach(
        (key, offset) -> {
          List<V> batch = new ArrayList<>();
          try (KeyValueIterator<BatchKey<K>, V> range =
              this.store.range(new BatchKey<>(key, 0L), new BatchKey<>(key, Long.MAX_VALUE))) {

            range.forEachRemaining(
                item -> {
                  batch.add(item.value);
                  itemsToDelete.add(item.key);
                });
          }
          this.context.forward(key, batch);
        });

    this.entries.clear();
    itemsToDelete.forEach(
        k -> this.store.delete(k));
  }

  @Override
  public void close() {
    this.store = null;
    this.entries = null;
    this.punctuation.cancel();
    this.punctuation = null;
  }
}
