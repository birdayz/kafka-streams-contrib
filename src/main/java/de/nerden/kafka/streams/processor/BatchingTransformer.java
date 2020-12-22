package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.BatchKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
  private final long batchMaxDurationMillis;
  private final long maxBatchSizePerKey;

  Cancellable punctuation;

  public BatchingTransformer(
      String storeName, long batchMaxDurationMillis, long maxBatchSizePerKey) {
    this.storeName = storeName;
    this.batchMaxDurationMillis = batchMaxDurationMillis;
    this.maxBatchSizePerKey = maxBatchSizePerKey;
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
            Duration.ofMillis(batchMaxDurationMillis),
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> forwardAll());
  }

  @Override
  public KeyValue<K, List<V>> transform(K key, V value) {
    // TODO check if batch would exceed message limit per key
    this.store.put(new BatchKey<>(key, this.context.offset()), value);
    this.entries.merge(key, 1L, Long::sum);

    if (this.entries.get(key) >= this.maxBatchSizePerKey) {
      // Forward already
      List<KeyValue<BatchKey<K>, V>> batch = getMessages(key);
      context.forward(key, batch.stream().map(kv -> kv.value).collect(Collectors.toList()));
      batch.forEach(kv -> this.store.delete(kv.key));
      this.entries.remove(key);
    }

    return null;
  }

  private void forwardAll() {
    List<BatchKey<K>> itemsToDelete = new ArrayList<>();
    this.entries.forEach(
        (key, offset) -> {
          List<KeyValue<BatchKey<K>, V>> batch = getMessages(key);
          this.context.forward(
              key, batch.stream().map(kv -> kv.value).collect(Collectors.toList()));
          batch.stream().map(kv -> kv.key).forEach(itemsToDelete::add);
        });

    this.entries.clear();
    itemsToDelete.forEach(k -> this.store.delete(k));
  }

  private List<KeyValue<BatchKey<K>, V>> getMessages(K key) {
    List<KeyValue<BatchKey<K>, V>> batch = new ArrayList<>();
    try (KeyValueIterator<BatchKey<K>, V> range =
        this.store.range(new BatchKey<>(key, 0L), new BatchKey<>(key, Long.MAX_VALUE))) {
      range.forEachRemaining(batch::add);
    }
    return batch;
  }

  @Override
  public void close() {
    this.store = null;
    this.entries = null;
    this.punctuation.cancel();
    this.punctuation = null;
  }
}
