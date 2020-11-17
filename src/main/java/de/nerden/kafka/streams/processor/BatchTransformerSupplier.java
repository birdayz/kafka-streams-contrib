package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.serde.BatchKeySerde;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class BatchTransformerSupplier<K, V>
    implements TransformerSupplier<K, V, KeyValue<K, List<V>>> {

  private String storeName;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;

  public BatchTransformerSupplier(String storeName, Serde<K> keySerde, Serde<V> valueSerde) {
    this.storeName = storeName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  @Override
  public Transformer<K, V, KeyValue<K, List<V>>> get() {
    return new BatchingTransformer<>(this.storeName);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return Collections.singleton(
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                new BatchKeySerde<>(this.keySerde),
                this.valueSerde)
            .withLoggingEnabled(Map.of()));
  }
}
