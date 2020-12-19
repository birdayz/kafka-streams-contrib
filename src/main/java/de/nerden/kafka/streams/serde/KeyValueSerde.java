package de.nerden.kafka.streams.serde;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.errors.SerializationException;
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
      if (data == null) {
        return null;
      }
      try {
        final de.nerden.kafka.streams.proto.KeyValue keyValue =
            de.nerden.kafka.streams.proto.KeyValue.parseFrom(data);
        return KeyValue.pair(
            this.keyDeserializer.deserialize(
                topic, keyValue.getKey() == null ? null : keyValue.getKey().toByteArray()),
            this.valueDeserializer.deserialize(
                topic, keyValue.getValue() == null ? null : keyValue.getValue().toByteArray()));
      } catch (InvalidProtocolBufferException e) {
        throw new SerializationException("Failed to deserialize proto", e);
      }
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
      if (data == null) {
        return null;
      }
      byte[] key = this.keySerializer.serialize(topic, data.key);
      byte[] value = this.valueSerializer.serialize(topic, data.value);

      return de.nerden.kafka.streams.proto.KeyValue.newBuilder()
          .setKey(key == null ? null : ByteString.copyFrom(key))
          .setValue(value == null ? null : ByteString.copyFrom(value))
          .build()
          .toByteArray();
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
