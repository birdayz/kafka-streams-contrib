package de.nerden.kafka.streams;

import de.nerden.kafka.streams.serde.KeyValueSerde;
import org.apache.kafka.common.serialization.Serde;

public class MoreSerdes {

  public static <K, V> KeyValueSerde<K, V> KeyValue(Serde<K> keySerde, Serde<V> valueSerde) {
    return new KeyValueSerde<>(keySerde, valueSerde);
  }
}
