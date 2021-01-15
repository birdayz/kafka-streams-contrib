package de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.processor.AsyncTransformer.AsyncMessage;
import de.nerden.kafka.streams.serde.AsyncMessageSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class AsyncTransformerSupplier<K, V> implements TransformerSupplier<K, V, KeyValue<K, V>> {


  private final String storeName;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final KeyValueBytesStoreSupplier storeSupplier;
  private final boolean changeLoggingEnabled;
  private final int maxInflight;
  private final int timeoutMs;

  private final Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn;
  private final Predicate<AsyncMessage<K, V>> retryDecider;

  public AsyncTransformerSupplier(
      String storeName,
      Serde<K> keySerde,
      Serde<V> valueSerde,
      KeyValueBytesStoreSupplier storeSupplier,
      boolean changeLoggingEnabled,
      int maxInflight,
      int timeoutMs,
      Function<KeyValue<K, V>, CompletableFuture<KeyValue<K, V>>> fn,
      Predicate<AsyncMessage<K, V>> retryDecider) {
    this.storeName = storeName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.storeSupplier = storeSupplier;
    this.changeLoggingEnabled = changeLoggingEnabled;
    this.maxInflight = maxInflight;
    this.timeoutMs = timeoutMs;
    this.fn = fn;
    this.retryDecider = retryDecider;
  }

  @Override
  public Transformer<K, V, KeyValue<K, V>> get() {
    return new AsyncTransformer<>(fn, retryDecider, this.storeName, this.maxInflight,
        this.timeoutMs);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    StoreBuilder<KeyValueStore<Long, AsyncMessage<K, V>>> builder;
    if (this.storeSupplier != null) {
      builder =
          Stores.keyValueStoreBuilder(
              this.storeSupplier, Serdes.Long(),
              new AsyncMessageSerde<>(this.keySerde, this.valueSerde));
    } else {
      builder =
          Stores.keyValueStoreBuilder(
              Stores.inMemoryKeyValueStore(storeName),
              Serdes.Long(),
              new AsyncMessageSerde<>(this.keySerde, this.valueSerde));
    }

    return Collections.singleton(
        changeLoggingEnabled
            ? builder.withLoggingEnabled(Map.of())
            : builder.withLoggingDisabled());
  }
}
