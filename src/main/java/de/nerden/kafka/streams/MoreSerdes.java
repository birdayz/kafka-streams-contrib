package de.nerden.kafka.streams;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import de.nerden.kafka.streams.serde.KeyValueSerde;
import de.nerden.kafka.streams.serde.ProtoSerde;
import org.apache.kafka.common.serialization.Serde;

public class MoreSerdes {

  public static <K, V> KeyValueSerde<K, V> KeyValue(Serde<K> keySerde, Serde<V> valueSerde) {
    return new KeyValueSerde<>(keySerde, valueSerde);
  }

  public static <T extends MessageLite> Serde<T> Proto(Parser<T> parser) {
    return new ProtoSerde<>(parser, false);
  }

  public static <T extends MessageLite> Serde<T> Proto(Parser<T> parser, boolean _throw) {
    return new ProtoSerde<>(parser, _throw);
  }
}
