package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.serde.KeyValueSerde;
import java.util.Set;
import java.util.concurrent.Executors;
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

  private final String inflightMessagesStoreName;
  private final String failedMessagesStoreName;
  private Consumer<KeyValue<K, V>> fn;
  private final StoreBuilder<KeyValueStore<Long, KeyValue<K, V>>> inflightMessagesStoreBuilder;
  private final StoreBuilder<KeyValueStore<Long, KeyValue<K, V>>> failedMessagesStoreBuilder;

  public AsyncProcessorSupplier(
      String inflightMessagesStoreName,
      String failedMessagesStoreName,
      Serde<K> keySerde,
      Serde<V> valueSerde,
      Consumer<KeyValue<K, V>> fn) {
    this.inflightMessagesStoreName = inflightMessagesStoreName;
    this.failedMessagesStoreName = failedMessagesStoreName;
    this.fn = fn;
    this.inflightMessagesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(inflightMessagesStoreName),
            Serdes.Long(),
            new KeyValueSerde<>(keySerde, valueSerde));
    this.failedMessagesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(failedMessagesStoreName),
            Serdes.Long(),
            new KeyValueSerde<>(keySerde, valueSerde));
  }

  @Override
  public Processor<K, V> get() {
    return new AsyncProcessor<>(
        inflightMessagesStoreName,
        failedMessagesStoreName,
        fn,
        () -> Executors.newFixedThreadPool(100));
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return Set.of(inflightMessagesStoreBuilder, failedMessagesStoreBuilder);
  }
}
