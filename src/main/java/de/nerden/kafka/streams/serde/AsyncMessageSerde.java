package de.nerden.kafka.streams.serde;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import de.nerden.kafka.streams.AsyncMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class AsyncMessageSerde<K, V> implements Serde<AsyncMessage<K, V>> {

  private final AsyncMessageDeserializer deserializer;
  private final AsyncMessageSerializer serializer;

  public AsyncMessageSerde(Serde<K> keySerde, Serde<V> valueSerde) {
    this.deserializer =
        new AsyncMessageDeserializer(keySerde.deserializer(), valueSerde.deserializer());
    this.serializer = new AsyncMessageSerializer(keySerde.serializer(), valueSerde.serializer());
  }

  private class AsyncMessageDeserializer implements Deserializer<AsyncMessage<K, V>> {
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public AsyncMessageDeserializer(
        Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
    }

    @Override
    public AsyncMessage<K, V> deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }

      try {
        de.nerden.kafka.streams.proto.AsyncMessage asyncMessage =
            de.nerden.kafka.streams.proto.AsyncMessage.parseFrom(data);

        return new AsyncMessage<K, V>(
            this.keyDeserializer.deserialize(
                topic, asyncMessage.hasKey() ? asyncMessage.getKey().toByteArray() : null),
            this.valueDeserializer.deserialize(
                topic, asyncMessage.hasValue() ? asyncMessage.getValue().toByteArray() : null),
            asyncMessage.getOffset(),
            asyncMessage.getNumFails());
      } catch (InvalidProtocolBufferException e) {
        throw new SerializationException("Failed to deserialize proto", e);
      }
    }
  }

  private class AsyncMessageSerializer implements Serializer<AsyncMessage<K, V>> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public AsyncMessageSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public byte[] serialize(String topic, AsyncMessage<K, V> asyncMessage) {
      if (asyncMessage == null) {
        return null;
      }

      byte[] key = this.keySerializer.serialize(topic, asyncMessage.getKey());
      byte[] value = this.valueSerializer.serialize(topic, asyncMessage.getValue());

      de.nerden.kafka.streams.proto.AsyncMessage.Builder builder =
          de.nerden.kafka.streams.proto.AsyncMessage.newBuilder();

      builder.setOffset(asyncMessage.getOffset());
      builder.setNumFails(asyncMessage.getNumFails());

      if (key != null) {
        builder.setKey(ByteString.copyFrom(key));
      }

      if (value != null) {
        builder.setValue(ByteString.copyFrom(value));
      }

      return builder.build().toByteArray();
    }
  }

  @Override
  public Serializer<AsyncMessage<K, V>> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<AsyncMessage<K, V>> deserializer() {
    return this.deserializer;
  }
}
