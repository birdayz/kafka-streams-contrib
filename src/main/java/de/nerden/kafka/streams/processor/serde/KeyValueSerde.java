package de.nerden.kafka.streams.processor.serde;

import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerde<K, V> implements Serde<KeyValue<K, V>> {

  private KeyValueSerializer keyValueSerializer;
  private KeyValueDeserializer keyValueDeserializer;

  public KeyValueSerde(Serde<K> keySerde, Serde<V> valueSerde) {
    this.keyValueSerializer =
        new KeyValueSerializer(keySerde.serializer(), valueSerde.serializer());
    this.keyValueDeserializer =
        new KeyValueDeserializer(keySerde.deserializer(), valueSerde.deserializer());
  }

  private class KeyValueDeserializer implements Deserializer<KeyValue<K, V>> {

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public KeyValueDeserializer(
        Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {

      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
    }

    @Override
    public KeyValue<K, V> deserialize(String topic, byte[] data) {

      ByteBuffer b = ByteBuffer.wrap(data);
      int keyLength = b.getInt();
      byte[] keyData = new byte[keyLength];
      b.get(keyData);
      K key = this.keyDeserializer.deserialize(topic, keyData);

      int valueLength = b.getInt();
      byte[] valueData = new byte[valueLength];
      b.get(valueData);
      V value = this.valueDeserializer.deserialize(topic, valueData);
      return KeyValue.pair(key, value);
    }
  }

  private class KeyValueSerializer implements Serializer<KeyValue<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public KeyValueSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public byte[] serialize(String topic, KeyValue<K, V> data) {
      byte[] key = this.keySerializer.serialize(topic, data.key);
      byte[] value = this.valueSerializer.serialize(topic, data.value);

      return ByteBuffer.allocate(key.length + value.length + 4 + 4)
          .putInt(key.length)
          .put(key)
          .putInt(value.length)
          .put(value)
          .array();
    }
  }

  @Override
  public Serializer<KeyValue<K, V>> serializer() {
    return this.keyValueSerializer;
  }

  @Override
  public Deserializer<KeyValue<K, V>> deserializer() {
    return this.keyValueDeserializer;
  }
}
