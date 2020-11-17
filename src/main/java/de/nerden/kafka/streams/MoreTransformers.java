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
    final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedI =
        new MaterializedInternal<>(materialized);
    String storeName;
    if (materializedI.storeName() != null) {
      storeName = materializedI.storeName();
    } else {
      storeName = "batch";
    }

    return new BatchTransformerSupplier<>(
        storeName, materializedI.keySerde(), materializedI.valueSerde());
  }
}
