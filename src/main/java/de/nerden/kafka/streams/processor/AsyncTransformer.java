package de.nerden.kafka.streams.processor;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class AsyncTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

  private ProcessorContext context;

  private final Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn;
  private final Predicate<AsyncMessage<K, V>> retryDecider;

  private final int maxInflight;
  private final int timeoutMs;

  private final String inflightStoreName;
  private KeyValueStore<Long, AsyncMessage<K, V>> inflightStore;

  ConcurrentLinkedQueue<Long> completedQueue;
  ConcurrentLinkedQueue<AsyncMessage<K, V>> failedQueue;

  private Semaphore semaphore;

  private Cancellable punctator;

  public AsyncTransformer(
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      Predicate<AsyncMessage<K, V>> retryDecider,
      String inflightStoreName,
      int maxInflight,
      int timeoutMs) {
    this.fn = fn;
    this.retryDecider = retryDecider;
    this.inflightStoreName = inflightStoreName;
    this.maxInflight = maxInflight;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public void init(ProcessorContext context) {
    this.semaphore = new Semaphore(this.maxInflight);
    this.context = context;

    this.completedQueue = new ConcurrentLinkedQueue<>();
    this.failedQueue = new ConcurrentLinkedQueue<>();

    this.inflightStore = context.getStateStore(this.inflightStoreName);

    // In-flight messages are considered failed after a restart.
    try (KeyValueIterator<Long, AsyncMessage<K, V>> i = this.inflightStore.all()) {
      i.forEachRemaining(
          kv -> {
            var msg = kv.value;
            msg.addFail(
                new InterruptedException(
                    "Message processing interrupted. It may or may not have been processed"));
            this.failedQueue.add(kv.value);
          });
    }

    // Run it regularly to clear successful often
    punctator =
        this.context.schedule(
            Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
  }

  @Override
  public KeyValue<K, V> transform(K key, V value) {
    handleFinished();
    runAsync(new AsyncMessage<>(key, value, context.offset(), 0, null));

    return null;
  }

  private void punctuate(long l) {
    handleFinished();
    handleFailed();
  }

  private void handleFinished() {
    Long offset;
    while ((offset = completedQueue.poll()) != null) {
      AsyncMessage<K, V> asyncMessage = this.inflightStore.get(offset);
      // TODO what if does not exist in inflight?
      if (asyncMessage == null) {
        throw new RuntimeException(
            "Could not find message in inflightstore..this should not happen");
      }
      context.forward(asyncMessage.getKey(), asyncMessage.getValue());
      this.inflightStore.delete(offset);
    }
  }

  private void handleFailed() {
    AsyncMessage<K, V> asyncMessage;
    while ((asyncMessage = failedQueue.poll()) != null) {
      if (this.retryDecider.test(asyncMessage)) {
        runAsync(asyncMessage);
      } else {
        // No retry -> drop it.
        this.inflightStore.delete(asyncMessage.getOffset());
      }
    }
  }

  // Exec func with the record as input, move entries to queues on completion/failure
  // Must only be called from StreamThread
  private void runAsync(AsyncMessage<K, V> asyncMessage) {
    K key = asyncMessage.getKey();
    V value = asyncMessage.getValue();
    long offset = this.context.offset();

    this.semaphore.acquireUninterruptibly();

    this.inflightStore.put(offset, asyncMessage);

    CompletableFuture<KeyValue<K, V>> cf = this.fn.apply(KeyValue.pair(key, value));

    cf.orTimeout(this.timeoutMs, TimeUnit.MILLISECONDS)
        .whenComplete(
            (result, e) -> {
              if (e == null) {
                this.completedQueue.add(offset);
              }

              if (e != null) {
                asyncMessage.addFail(e);
                this.failedQueue.add(asyncMessage);
              }

              this.semaphore.release();
            });
  }

  @Override
  public void close() {
    if (punctator != null) {
      punctator.cancel();
    }

    this.completedQueue = null;
    this.failedQueue = null;
  }

  public static class AsyncMessage<K, V> {

    private final K key;
    private final V value;
    private final long offset;
    private int numFails;
    private Throwable exception;

    public AsyncMessage(K key, V value, long offset, int numFails, Throwable exception) {
      this.key = key;
      this.value = value;
      this.offset = offset;
      this.numFails = numFails;
      this.exception = exception;
    }

    public V getValue() {
      return value;
    }

    public long getOffset() {
      return offset;
    }

    public int getNumFails() {
      return numFails;
    }

    public K getKey() {
      return key;
    }

    public Throwable getThrowable() {
      return exception;
    }

    public void addFail(Throwable t) {
      this.exception = t;
      this.numFails++;
    }
  }
}
