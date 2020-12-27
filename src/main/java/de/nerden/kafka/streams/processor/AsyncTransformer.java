package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.RetryMessage;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public class AsyncTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {
  private ProcessorContext context;

  private Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn;
  private BiPredicate<RetryMessage<K, V>, Throwable> retryDecider;

  private final String inflightStoreName;
  //    private final String failedStoreName;
  private final int maxInflight;
  private final int timeoutMs;

  private KeyValueStore<Long, KeyValue<K, V>> inflightMessages;
  //  private KeyValueStore<Long, RetryMessage<K, V>> failedMessages;

  private Semaphore semaphore;

  public AsyncTransformer(
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      //          ,
      BiPredicate<RetryMessage<K, V>, Throwable> retryDecider,
      //          ,
      String inflightStoreName,
      //      String failedStoreName,
      int maxInflight,
      int timeoutMs) { // TODO: Decider func
    this.fn = fn;
    this.retryDecider = retryDecider;
    this.inflightStoreName = inflightStoreName;
    //    this.failedStoreName = failedStoreName;
    this.maxInflight = maxInflight;
    this.timeoutMs = timeoutMs;
  }

  private void handleFinished() {
    KeyValue<K, V> kv;
    while ((kv = completedQueue.poll()) != null) {
      System.out.println(kv.toString());
      context.forward(kv.key, kv.value);
      // TODO Remove from inflight
    }

    RetryMessage<K, V> retry;
    while ((retry = retryQueue.poll()) != null) {
      // Exec again
      processAsync(retry.getKey(), retry.getValue());
    }
  }

  @Override
  public void init(ProcessorContext context) {
    this.semaphore = new Semaphore(this.maxInflight);
    this.context = context;

    this.inflightMessages = context.getStateStore(this.inflightStoreName);

    this.context.schedule(
        Duration.ofMillis(1000),
        PunctuationType.WALL_CLOCK_TIME,
        timestamp -> {
          System.out.println("punctuate called");
          handleFinished();
        });
  }

  ConcurrentLinkedQueue<KeyValue<K, V>> completedQueue = new ConcurrentLinkedQueue<>();
  ConcurrentLinkedQueue<RetryMessage<K, V>> retryQueue = new ConcurrentLinkedQueue<>();

  @Override
  public KeyValue<K, V> transform(K key, V value) {
    handleFinished();
    processAsync(key, value);

    return null;
  }

  // Exec func with the record as input, move entries to queues on completion/failure
  private void processAsync(K key, V value) {
    this.semaphore.acquireUninterruptibly();
    CompletableFuture<KeyValue<K, V>> cf = this.fn.apply(KeyValue.pair(key, value));

    cf.orTimeout(this.timeoutMs, TimeUnit.MILLISECONDS)
        .whenComplete(
            (result, e) -> {
              if (e == null) {
                System.out.println("OK"+ result.toString());
                completedQueue.add(result);
              }

              if (e != null) {
                RetryMessage<K, V> retry = new RetryMessage<>(key, value, context.offset(), 1);

                if (this.retryDecider.test(retry, e)) {
                  this.retryQueue.add(retry);
                  System.out.println("Retrying");
                } else {
                  System.out.println("Not retrying");
                }
              }

              this.semaphore.release();
            });
  }

  @Override
  public void close() {}
}
