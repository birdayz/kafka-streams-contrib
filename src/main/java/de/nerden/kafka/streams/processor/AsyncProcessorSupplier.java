package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.serde.KeyValueSerde;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class AsyncProcessorSupplier<K, V> implements ProcessorSupplier<K, V> {

  private final String storeName;
  private Consumer<KeyValue<K, V>> fn;
  private final StoreBuilder<KeyValueStore<Long, KeyValue<K, V>>> storeBuilder;

  public AsyncProcessorSupplier(
      String storeName, Serde<K> keySerde, Serde<V> valueSerde, Consumer<KeyValue<K, V>> fn) {
    this.storeName = storeName;
    this.fn = fn;
    this.storeBuilder =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(storeName),
            Serdes.Long(),
            new KeyValueSerde<>(keySerde, valueSerde));
  }

  @Override
  public Processor<K, V> get() {
    return new AsyncProcessor<>(storeName, fn);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return Collections.singleton(storeBuilder);
  }
}
