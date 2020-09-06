package de.nerden.kafka.streams.processor;

import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProcessor<K, V> implements Processor<K, V> {

  private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

  private ExecutorService executorService;

  private String storeName;
  private final Consumer<KeyValue<K, V>> fn;

  private final List<Long> inflight;

  public AsyncProcessor(String storeName, Consumer<KeyValue<K, V>> fn) {
    this.storeName = storeName;
    this.fn = fn;
    this.executorService = Executors.newCachedThreadPool();
    this.inflight = Collections.synchronizedList(new ArrayList<>());
  }

  private KeyValueStore<Long, KeyValue<K, V>> store;
  private ProcessorContext context;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    store = (KeyValueStore<Long, KeyValue<K, V>>) context.getStateStore(this.storeName);
    this.context = context;
    this.executorService = Executors.newCachedThreadPool();
    context.schedule(
        Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, timestamp -> this.punctuate());
  }

  private void punctuate() {
    KeyValueIterator<Long, KeyValue<K, V>> i = this.store.all();

    i.forEachRemaining(
        kv -> {
          if (this.inflight.contains(kv.key)) {
            // Skip if inflight
            return;
          }

          execAsync(kv);
        });

    i.close();
  }

  private void execAsync(KeyValue<Long, KeyValue<K, V>> kv) {
    this.inflight.add(kv.key);
    executorService.execute(
        () -> {
          try {
            this.fn.accept(kv.value);
            this.store.delete(kv.key);
          } finally {
            this.inflight.remove(kv.key);
          }
        });
  }

  @Override
  public void process(K key, V value) {
    this.store.put(this.context.offset(), KeyValue.pair(key, value));
    execAsync(KeyValue.pair(this.context.offset(), KeyValue.pair(key, value)));
  }

  @Override
  public void close() {
    boolean closedSuccessfully =
        MoreExecutors.shutdownAndAwaitTermination(executorService, 5000, TimeUnit.MILLISECONDS);
    if (!closedSuccessfully) {
      log.error("Failed to shut down ExecutorService");
    }
    this.executorService = null;
  }
}
