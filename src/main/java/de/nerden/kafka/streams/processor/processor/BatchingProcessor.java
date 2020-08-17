package de.nerden.kafka.streams.processor.processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class BatchingProcessor<K, V> implements Processor<K, V> {

  private KeyValueStore<Long, KeyValue<K, V>> store;
  private ProcessorContext context;

  @Override @SuppressWarnings("unchecked") public void init(final ProcessorContext context) {
    store = (KeyValueStore<Long, KeyValue<K, V>>) context.getStateStore("batch");
    this.context = context;
    this.context.schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
      forwardBatch();
    });
  }

  @Override public void process(final K key, final V value) {
    this.store.put(context.offset(), KeyValue.pair(key, value));
  }

  private void forwardBatch() {
    final KeyValueIterator<Long, KeyValue<K, V>> all = this.store.all();

    // TODO all() does not guarantee ordering, but we want to keep ordering if possible

    List<KeyValue<K, V>> res = new ArrayList<>();

    all.forEachRemaining(longKeyValueKeyValue -> {
      res.add(longKeyValueKeyValue.value);
    });

    all.close();

    if (!res.isEmpty()) {
      context.forward(null, res);
    }

    KeyValueIterator<Long, KeyValue<K, V>> i = store.all();

    i.forEachRemaining(longKeyValueKeyValue -> {
      store.delete(longKeyValueKeyValue.key);
    });

    i.close();
  }

  @Override public void close() {
  }
}
