package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.AsyncMessage;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class AsyncTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {
  private ProcessorContext context;

  private Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn;
  private BiPredicate<AsyncMessage<K, V>, Throwable> retryDecider;

  private final int maxInflight;
  private final int timeoutMs;

  private final String inflightStoreName;
  private final String failedStoreName;
  private KeyValueStore<Long, AsyncMessage<K, V>> inflightStore;
  private KeyValueStore<Long, AsyncMessage<K, V>> failedStore;

  ConcurrentLinkedQueue<Long> completedQueue = new ConcurrentLinkedQueue<>();
  ConcurrentLinkedQueue<AsyncMessage<K, V>> failedQueue = new ConcurrentLinkedQueue<>();

  private Semaphore semaphore;

  public AsyncTransformer(
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      BiPredicate<AsyncMessage<K, V>, Throwable> retryDecider,
      String inflightStoreName,
      String failedStoreName,
      int maxInflight,
      int timeoutMs) { // TODO: Decider func
    this.fn = fn;
    this.retryDecider = retryDecider;
    this.inflightStoreName = inflightStoreName;
    this.failedStoreName = failedStoreName;
    this.maxInflight = maxInflight;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public void init(ProcessorContext context) {
    this.semaphore = new Semaphore(this.maxInflight);
    this.context = context;

    this.inflightStore = context.getStateStore(this.inflightStoreName);
    this.failedStore = context.getStateStore(this.failedStoreName);

    this.context.schedule(
        Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
  }

  private void handleFinished() {
    {
      Long offset;
      while ((offset = completedQueue.poll()) != null) {
        AsyncMessage<K, V> asyncMessage = this.inflightStore.get(offset);
        context.forward(asyncMessage.getKey(), asyncMessage.getValue());
        this.inflightStore.delete(offset);
      }
    }

    // TODO
    {
      AsyncMessage<K, V> asyncMessage;
      while ((asyncMessage = failedQueue.poll()) != null) {
        // Move to retry store. The actual retry is performed somewhere else
        this.failedStore.put(asyncMessage.getOffset(), asyncMessage);
        this.inflightStore.delete(asyncMessage.getOffset());
      }
    }
  }

  private void punctuate(long l) {
    handleFinished();

    try (KeyValueIterator<Long, AsyncMessage<K, V>> all = this.failedStore.all()) {
      while (all.hasNext()) {
        KeyValue<Long, AsyncMessage<K, V>> item = all.next();
        this.runAsync(item.value);
      }
    }
  }

  @Override
  public KeyValue<K, V> transform(K key, V value) {
    handleFinished();
    runAsync(new AsyncMessage<>(key, value, context.offset(), 0));

    return null;
  }

  // Exec func with the record as input, move entries to queues on completion/failure
  // Must only be called from StreamThread
  private void runAsync(AsyncMessage<K, V> asyncMessage) {
    K key = asyncMessage.getKey();
    V value = asyncMessage.getValue();

    this.semaphore.acquireUninterruptibly();

    long offset = this.context.offset();

    this.inflightStore.put(offset, asyncMessage);

    CompletableFuture<KeyValue<K, V>> cf = this.fn.apply(KeyValue.pair(key, value));

    cf.orTimeout(this.timeoutMs, TimeUnit.MILLISECONDS)
        .whenComplete(
            (result, e) -> {
              if (e == null) {
                this.completedQueue.add(offset);
              }

              if (e != null) {
                AsyncMessage<K, V> retry = new AsyncMessage<>(key, value, context.offset(), 1);

                // TODO move decider to the place where it's read from fail-store? Probably better
                if (this.retryDecider.test(retry, e)) {
                  this.failedQueue.add(retry);
                }
              }

              this.semaphore.release();
            });
  }

  @Override
  public void close() {}
}
