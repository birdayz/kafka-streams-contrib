package de.nerden.kafka.streams.processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  private List<Long> inflight;

  public AsyncProcessor(Consumer<KeyValue<K, V>> fn) {
    this.fn = fn;
    this.executor = Executors.newCachedThreadPool();
    this.inflight = Collections.synchronizedList(new ArrayList<>());
  }

  private KeyValueStore<Long, KeyValue<K, V>> store;
  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    store = (KeyValueStore<Long, KeyValue<K, V>>) context.getStateStore("batch");
    this.context = context;
    context.schedule(
        Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, timestamp -> this.processAsync());
  }

  private void processAsync() {
    KeyValueIterator<Long, KeyValue<K, V>> i = this.store.all();

    i.forEachRemaining(
        kv -> {
          // Does not work, must store offset?          Long off = this.context.offset();
          this.inflight.add(kv.key);
          executor.execute(
              () -> {
                this.fn.accept(kv.value);
                this.store.delete(kv.key);
                this.inflight.remove(kv.key);
              });
        });

    i.close();
  }

  @Override
  public void process(K key, V value) {
    this.store.put(this.context.offset(), KeyValue.pair(key, value));
    this.inflight.add(this.context.offset());
    executor.execute(
        () -> {
          this.fn.accept(KeyValue.pair(key, value));
          this.store.delete(this.context.offset());
          this.inflight.remove(this.context.offset());
        });
  }

  @Override
  public void close() {}
}
