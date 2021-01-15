package de.nerden.kafka.streams;

import de.nerden.kafka.streams.processor.AsyncTransformer.AsyncMessage;
import de.nerden.kafka.streams.processor.AsyncTransformerSupplier;
import de.nerden.kafka.streams.processor.BatchTransformerSupplier;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.state.KeyValueStore;

public class MoreTransformers {

  public static final long DEFAULT_MAX_BATCH_DURATION_MILLIS = 10000L;
  public static final long DEFAULT_MAX_BATCH_SIZE_PER_KEY = 1000L;

  public static final int DEFAULT_ASYNC_MAX_INFLIGHT = 10;
  public static final int DEFAULT_ASYNC_TIMEOUT_MS = 10000;

  public static <K, V> TransformerSupplier<K, V, KeyValue<K, V>> Async(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized,
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      Predicate<AsyncMessage<K, V>> retryDecider) {
    return Async(materialized, fn, retryDecider,
        DEFAULT_ASYNC_MAX_INFLIGHT,
        DEFAULT_ASYNC_TIMEOUT_MS);
  }

  public static <K, V> TransformerSupplier<K, V, KeyValue<K, V>> Async(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized,
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      Predicate<AsyncMessage<K, V>> retryDecider,
      int maxInflight,
      int timeoutMs
  ) {
    final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
        new MaterializedInternal<>(materialized);

    if (materializedInternal.storeName() != null) {
      return new AsyncTransformerSupplier<>(
          materializedInternal.storeName(),
          materializedInternal.keySerde(),
          materializedInternal.valueSerde(),
          null,
          materializedInternal.loggingEnabled(),
          maxInflight,
          timeoutMs,
          fn,
          retryDecider
      );
    } else {
      return new AsyncTransformerSupplier<>(
          "async",
          materializedInternal.keySerde(),
          materializedInternal.valueSerde(),
          null,
          materializedInternal.loggingEnabled(),
          maxInflight,
          timeoutMs,
          fn,
          retryDecider
      );
    }

  }


  public static <K, V> TransformerSupplier<K, V, KeyValue<K, List<V>>> Batch(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
    return Batch(materialized, DEFAULT_MAX_BATCH_DURATION_MILLIS, DEFAULT_MAX_BATCH_SIZE_PER_KEY);
  }

  /**
   * @param materialized           Materialization information for the state stored used for
   *                               batching.
   * @param maxBatchDurationMillis Every time maxBatchDurationMillis passed, batches are released
   *                               for all keys.
   * @param maxBatchSizePerKey     When matchBatchSizePerKey records have been collected for a
   *                               specific key, the batch is forwarded be waited for.
   * @return
   */
  public static <K, V> TransformerSupplier<K, V, KeyValue<K, List<V>>> Batch(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized,
      long maxBatchDurationMillis,
      long maxBatchSizePerKey) {
    final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
        new MaterializedInternal<>(materialized);

    if (materializedInternal.storeName() != null) {
      return new BatchTransformerSupplier<>(
          materializedInternal.storeName(),
          materializedInternal.keySerde(),
          materializedInternal.valueSerde(),
          materializedInternal.loggingEnabled(),
          maxBatchDurationMillis,
          maxBatchSizePerKey);
    } else if (materializedInternal.storeSupplier() != null) {
      return new BatchTransformerSupplier<>(
          materializedInternal.storeName(),
          materializedInternal.keySerde(),
          materializedInternal.valueSerde(),
          materializedInternal.loggingEnabled(),
          maxBatchDurationMillis,
          maxBatchSizePerKey);
    } else {
      return new BatchTransformerSupplier<>(
          "batch",
          materializedInternal.keySerde(),
          materializedInternal.valueSerde(),
          materializedInternal.loggingEnabled(),
          maxBatchDurationMillis,
          maxBatchSizePerKey);
    }
  }
}
