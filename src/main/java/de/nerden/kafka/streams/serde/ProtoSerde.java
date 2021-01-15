package de.nerden.kafka.streams.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

public class ProtoSerde<T extends MessageLite> extends WrapperSerde<T> {

  /**
   * Throws exception if _throw is true, otherwise returns null
   *
   * @param parser Parser obtained by calling MyProtoType.parser()
   * @param _throw Throw exception in case of deserialization errors
   */
  public ProtoSerde(Parser<T> parser, boolean _throw) {
    super(new ProtoSerializer<>(), new ProtoDeserializer<>(parser, _throw));
  }

  static class ProtoSerializer<T extends MessageLite> implements Serializer<T> {

    ProtoSerializer() {}

    @Override
    public byte[] serialize(String topic, T data) {
      if (data != null) {
        return data.toByteArray();
      }
      return null;
    }
  }

  static class ProtoDeserializer<T extends MessageLite> implements Deserializer<T> {

    private final Parser<T> parser;
    private final boolean _throw;

    ProtoDeserializer(Parser<T> parser, boolean _throw) {
      this.parser = parser;
      this._throw = _throw;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      if (data != null) {
        try {
          return this.parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
          if (this._throw) {
            throw new SerializationException(e);
          } else {
            return null;
          }
        }
      }
      return null;
    }
  }
}
