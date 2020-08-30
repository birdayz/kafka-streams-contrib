package de.nerden.kafka.streams.processor;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class AsyncProcessor<K, V> implements Processor<K, V> {

  private Executor executor;

  private Consumer<KeyValue<K, V>> fn;

  public AsyncProcessor(Consumer<KeyValue<K, V>> fn) {
    this.fn = fn;
    this.executor = Executors.newCachedThreadPool();
  }

  private KeyValueStore<K, V> store;

  @Override
  public void init(ProcessorContext context) {
    store = (KeyValueStore<K, V>) context.getStateStore("batch");
    context.schedule(
        Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, timestamp -> this.processAsync());
  }

  private void processAsync() {
    KeyValueIterator<K, V> i = this.store.all();

    i.forEachRemaining(
        kv ->
            executor.execute(
                () -> {
                  this.fn.accept(kv);
                  this.store.delete(kv.key);
                }));

    i.close();
  }

  @Override
  public void process(K key, V value) {
    this.store.put(key, value);
  }

  @Override
  public void close() {}
}
