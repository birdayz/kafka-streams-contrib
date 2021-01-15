package de.nerden.kafka.streams.serde;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import de.nerden.kafka.streams.processor.BatchingTransformer.BatchKey;
import java.util.Base64;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class BatchKeySerde<K> implements Serde<BatchKey<K>> {

  private BatchEntryKeySerializer serializer;
  private BatchEntryKeyDeserializer deserializer;

  public BatchKeySerde(Serde<K> keySerde) {
    this.deserializer = new BatchEntryKeyDeserializer(keySerde.deserializer());
    this.serializer = new BatchEntryKeySerializer(keySerde.serializer());
  }

  private class BatchEntryKeyDeserializer implements Deserializer<BatchKey<K>> {

    private final Deserializer<K> keyDeserializer;

    public BatchEntryKeyDeserializer(Deserializer<K> keyDeserializer) {

      this.keyDeserializer = keyDeserializer;
    }

    @Override
    public BatchKey<K> deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }
      try {
        final de.nerden.kafka.streams.proto.BatchKey proto =
            de.nerden.kafka.streams.proto.BatchKey.parseFrom(data);

        byte[] decoded = Base64.getDecoder().decode(proto.getOriginalKey().toByteArray());

        BatchKey<K> kBatchKey =
            new BatchKey<>(keyDeserializer.deserialize(topic, decoded), proto.getOffset());
        return kBatchKey;
      } catch (InvalidProtocolBufferException e) {
        throw new SerializationException("Failed to deserialize proto", e);
      }
    }
  }

  private class BatchEntryKeySerializer implements Serializer<BatchKey<K>> {

    private final Serializer<K> keySerializer;

    public BatchEntryKeySerializer(Serializer<K> keySerializer) {
      this.keySerializer = keySerializer;
    }

    @Override
    public byte[] serialize(String topic, BatchKey<K> data) {
      if (data == null) {
        return null;
      }
      byte[] originalKey = this.keySerializer.serialize(topic, data.getKey());

      de.nerden.kafka.streams.proto.BatchKey proto =
          de.nerden.kafka.streams.proto.BatchKey.newBuilder()
              .setOriginalKey(
                  originalKey == null
                      ? null
                      : ByteString.copyFrom(Base64.getEncoder().encode(originalKey)))
              .setOffset(data.getOffset())
              .build();
      return proto.toByteArray();
    }
  }

  @Override
  public Serializer<BatchKey<K>> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<BatchKey<K>> deserializer() {
    return this.deserializer;
  }
}
