package de.nerden.kafka.streams;

import de.nerden.kafka.streams.processor.BatchTransformerSupplier;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.state.KeyValueStore;

public class MoreTransformers {

  public static <K, V> TransformerSupplier<K, V, KeyValue<K, List<V>>> Batch(
      Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
    return Batch(materialized, 10000L, 1000L);
  }

  /**
   * @param materialized Materialization information
   * @param maxBatchDurationMillis This is the upper bounds in terms of time, how long a batch will
   *     be waited for.
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
