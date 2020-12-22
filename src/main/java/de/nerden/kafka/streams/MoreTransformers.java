package de.nerden.kafka.streams;

import de.nerden.kafka.streams.processor.BatchTransformerSupplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class MoreTransformers {

  public static final long DEFAULT_MAX_BATCH_DURATION_MILLIS = 10000L;
  public static final long DEFAULT_MAX_BATCH_SIZE_PER_KEY = 1000L;

  public static <K, V> TransformerSupplier<K, V, KeyValue<K, List<V>>> Batch(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
    return Batch(materialized, DEFAULT_MAX_BATCH_DURATION_MILLIS, DEFAULT_MAX_BATCH_SIZE_PER_KEY);
  }

  /**
   * @param materialized Materialization information for the state stored used for batching.
   * @param maxBatchDurationMillis Every time maxBatchDurationMillis passed, batches are released
   *     for all keys.
   * @param maxBatchSizePerKey When matchBatchSizePerKey records have been collected for a specific
   *     key, the batch is forwarded be waited for.
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
