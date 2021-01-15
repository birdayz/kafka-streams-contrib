package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.processor.BatchingTransformer.BatchKey;
import de.nerden.kafka.streams.serde.BatchKeySerde;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.*;

public class BatchTransformerSupplier<K, V>
    implements TransformerSupplier<K, V, KeyValue<K, List<V>>> {

  private String storeName;
  private final KeyValueBytesStoreSupplier storeSupplier;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;
  private final boolean changeLoggingEnabled;
  private final long maxBatchDurationMillis;
  private final long maxBatchSizePerKey;

  public BatchTransformerSupplier(
      KeyValueBytesStoreSupplier storeSupplier,
      Serde<K> keySerde,
      Serde<V> valueSerde,
      boolean changeLoggingEnabled,
      long maxBatchDurationMillis,
      long maxBatchSizePerKey) {
    this.storeSupplier = storeSupplier;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.changeLoggingEnabled = changeLoggingEnabled;
    this.maxBatchDurationMillis = maxBatchDurationMillis;
    this.maxBatchSizePerKey = maxBatchSizePerKey;
  }

  public BatchTransformerSupplier(
      String storeName,
      Serde<K> keySerde,
      Serde<V> valueSerde,
      boolean changeLoggingEnabled,
      long maxBatchDurationMillis,
      long maxBatchSizePerKey) {
    this.storeName = storeName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.changeLoggingEnabled = changeLoggingEnabled;
    this.storeSupplier = null;
    this.maxBatchDurationMillis = maxBatchDurationMillis;
    this.maxBatchSizePerKey = maxBatchSizePerKey;
  }

  @Override
  public Transformer<K, V, KeyValue<K, List<V>>> get() {
    return new BatchingTransformer<>(
        this.storeName, this.maxBatchDurationMillis, this.maxBatchSizePerKey);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {

    StoreBuilder<KeyValueStore<BatchKey<K>, V>> builder;
    if (this.storeSupplier != null) {
      builder =
          Stores.keyValueStoreBuilder(
              this.storeSupplier, new BatchKeySerde<>(this.keySerde), this.valueSerde);
    } else {
      builder =
          Stores.keyValueStoreBuilder(
              Stores.inMemoryKeyValueStore(storeName),
              new BatchKeySerde<>(this.keySerde),
              this.valueSerde);
    }

    return Collections.singleton(
        changeLoggingEnabled
            ? builder.withLoggingEnabled(Map.of())
            : builder.withLoggingDisabled());
  }
}
