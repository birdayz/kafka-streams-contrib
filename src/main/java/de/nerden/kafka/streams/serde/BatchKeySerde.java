package de.nerden.kafka.streams.serde;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import de.nerden.kafka.streams.BatchEntryKey;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class BatchKeySerde<K> implements Serde<BatchEntryKey<K>> {

  private BatchEntryKeySerializer serializer;
  private BatchEntryKeyDeserializer deserializer;

  public BatchKeySerde(Serde<K> keySerde) {
    this.deserializer = new BatchEntryKeyDeserializer(keySerde.deserializer());
    this.serializer = new BatchEntryKeySerializer(keySerde.serializer());
  }

  private class BatchEntryKeyDeserializer implements Deserializer<BatchEntryKey<K>> {

    private final Deserializer<K> keyDeserializer;

    public BatchEntryKeyDeserializer(Deserializer<K> keyDeserializer) {

      this.keyDeserializer = keyDeserializer;
    }

    @Override
    public BatchEntryKey<K> deserialize(String topic, byte[] data) {
      try {
        final de.nerden.kafka.streams.proto.BatchKey proto =
            de.nerden.kafka.streams.proto.BatchKey.parseFrom(data);

        return new BatchEntryKey<>(
            keyDeserializer.deserialize(topic, proto.getOriginalKey().toByteArray()),
            proto.getOffset());
      } catch (InvalidProtocolBufferException e) {
        return null;
      }
    }
  }

  private class BatchEntryKeySerializer implements Serializer<BatchEntryKey<K>> {

    private final Serializer<K> keySerializer;

    public BatchEntryKeySerializer(Serializer<K> keySerializer) {
      this.keySerializer = keySerializer;
    }

    @Override
    public byte[] serialize(String topic, BatchEntryKey<K> data) {
      byte[] originKey = this.keySerializer.serialize(topic, data.getKey());
      de.nerden.kafka.streams.proto.BatchKey proto =
          de.nerden.kafka.streams.proto.BatchKey.newBuilder()
              .setOriginalKey(ByteString.copyFrom(originKey))
              .setOffset(data.getOffset())
              .build();
      return proto.toByteArray();
    }
  }

  @Override
  public Serializer<BatchEntryKey<K>> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<BatchEntryKey<K>> deserializer() {
    return this.deserializer;
  }
}
