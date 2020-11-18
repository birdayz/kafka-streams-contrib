package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.BatchKey;
import de.nerden.kafka.streams.serde.BatchKeySerde;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class BatchTransformerSupplier<K, V>
    implements TransformerSupplier<K, V, KeyValue<K, List<V>>> {

  private String storeName;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;
  private final boolean changeLoggingEnabled;

  public BatchTransformerSupplier(
      String storeName, Serde<K> keySerde, Serde<V> valueSerde, boolean changeLoggingEnabled) {
    this.storeName = storeName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.changeLoggingEnabled = changeLoggingEnabled;
  }

  @Override
  public Transformer<K, V, KeyValue<K, List<V>>> get() {
    return new BatchingTransformer<>(this.storeName);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    StoreBuilder<KeyValueStore<BatchKey<K>, V>> builder =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(storeName),
            new BatchKeySerde<>(this.keySerde),
            this.valueSerde);
    return Collections.singleton(
        changeLoggingEnabled
            ? builder.withLoggingEnabled(Map.of())
            : builder.withLoggingDisabled());
  }
}
