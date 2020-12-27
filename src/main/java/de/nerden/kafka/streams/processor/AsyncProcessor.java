package de.nerden.kafka.streams.processor;

import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProcessor<K, V, Kout, Vout> implements Processor<K, V, Kout, Vout> {

  private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);
  public static final int MAX_INFLIGHT = 100;

  private ExecutorService executorService;

  private final String inflightMessagesStoreName;
  private final String failedMessagesStoreName;
  private final Consumer<KeyValue<K, V>> fn;

  private Semaphore semaphore;
  private Supplier<ExecutorService> executorBuilder;

  public AsyncProcessor(Consumer<KeyValue<K, V>> fn, Supplier<ExecutorService> executorBuilder) {
    this("async-inflight", "async-failed", fn, executorBuilder);
  }

  public AsyncProcessor(
      String inflightMessagesStoreName,
      String failedMessagesStoreName,
      Consumer<KeyValue<K, V>> fn,
      Supplier<ExecutorService> executorBuilder) {
    this.inflightMessagesStoreName = inflightMessagesStoreName;
    this.failedMessagesStoreName = failedMessagesStoreName;
    this.fn = fn;
    this.executorBuilder = executorBuilder;
  }

  private KeyValueStore<Long, KeyValue<K, V>> inflightMessages;
  private KeyValueStore<Long, KeyValue<K, V>> failedMessages;
  private ProcessorContext<Kout, Vout> context;

  @Override
  public void init(ProcessorContext<Kout, Vout> context) {
    this.semaphore = new Semaphore(MAX_INFLIGHT);

    inflightMessages = context.getStateStore(this.inflightMessagesStoreName);
    failedMessages = context.getStateStore(this.failedMessagesStoreName);

    this.context = context;
    this.executorService = this.executorBuilder.get();

    context.schedule(
        Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, timestamp -> this.punctuate());
  }

  private void punctuate() {
    try (KeyValueIterator<Long, KeyValue<K, V>> i = this.failedMessages.all()) {
      while (i.hasNext()) {
        KeyValue<Long, KeyValue<K, V>> next = i.next();
        boolean ok = this.semaphore.tryAcquire();
        if (ok) {
          this.failedMessages.delete(next.key);
          this.inflightMessages.put(next.key, next.value);
          execAsync(next);
        } else {
          break;
        }
      }
    }
  }

  private void execAsync(KeyValue<Long, KeyValue<K, V>> kv) {

    CompletableFuture.runAsync(
        () -> fn.accept(kv.value),
        executorService).whenComplete((aVoid, throwable) -> {

    });

    executorService.submit(
        () -> {
          try {
            this.fn.accept(kv.value);
          } catch (Throwable t) {
            this.failedMessages.put(kv.key, kv.value);
          } finally {
            this.inflightMessages.delete(kv.key);
            this.semaphore.release();
          }
        });
  }

  @Override
  public void process(Record<K, V> record) {
    this.semaphore.acquireUninterruptibly();
    this.inflightMessages.put(
        this.context.recordMetadata().get().offset(), KeyValue.pair(record.key(), record.value()));
    execAsync(
        KeyValue.pair(
            this.context.recordMetadata().get().offset(),
            KeyValue.pair(record.key(), record.value())));
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
