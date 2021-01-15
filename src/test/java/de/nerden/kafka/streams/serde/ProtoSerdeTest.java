package de.nerden.kafka.streams.serde;

import static com.google.common.truth.Truth.assertThat;

import com.google.protobuf.ByteString;
import de.nerden.kafka.streams.MoreSerdes;
import de.nerden.kafka.streams.proto.KeyValue;
import org.junit.jupiter.api.Test;

class ProtoSerdeTest {

  @Test
  void SerializeDeserialize() {
    var serde = MoreSerdes.Proto(KeyValue.parser());

    var input = KeyValue.newBuilder().setKey(ByteString.copyFrom(new byte[] {1, 2})).build();

    var serialized = serde.serializer().serialize("", input);
    var deserialized = serde.deserializer().deserialize("", serialized);

    assertThat(deserialized).isEqualTo(input);
  }
}
