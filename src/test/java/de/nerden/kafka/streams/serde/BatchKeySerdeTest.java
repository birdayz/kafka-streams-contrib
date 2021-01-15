package de.nerden.kafka.streams.serde;

import com.google.common.truth.Truth;
import de.nerden.kafka.streams.processor.BatchingTransformer.BatchKey;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BatchKeySerdeTest {

  @Test
  @DisplayName("Serializing and deserializing produces equal object")
  public void TestSerializeDeserialize() {
    BatchKeySerde<String> serde = new BatchKeySerde<>(Serdes.String());
    BatchKey<String> input = new BatchKey<>("test-key", 0L);

    byte[] serialized = serde.serializer().serialize("", input);
    BatchKey<String> deserialized = serde.deserializer().deserialize("", serialized);

    Truth.assertThat(deserialized).isEqualTo(input);
  }

  @Test
  @DisplayName("Serializing null produces null byte[]")
  public void TestSerializeNullProducesNull() {
    BatchKeySerde<String> serde = new BatchKeySerde<>(Serdes.String());
    byte[] serialized = serde.serializer().serialize("", null);

    Truth.assertThat(serialized).isNull();
  }

  @Test
  @DisplayName("Deserializing null produces null byte[]")
  public void TestDeserializeNullProducesNull() {
    BatchKeySerde<String> serde = new BatchKeySerde<>(Serdes.String());
    BatchKey<String> deserialized = serde.deserializer().deserialize("", null);

    Truth.assertThat(deserialized).isNull();
  }
}
