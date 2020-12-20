package de.nerden.kafka.streams.serde;

import com.google.common.truth.Truth;
import de.nerden.kafka.streams.MoreSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KeyValueSerdeTest {

  @Test
  @DisplayName("Serialize and deserialize String/String KeyValue")
  void TestStringKeyValue() {
    KeyValueSerde<String, String> serde = MoreSerdes.KeyValue(Serdes.String(), Serdes.String());

    String key = "test-key";
    String value = "test-value";

    KeyValue<String, String> kv = new KeyValue<>(key, value);

    byte[] serialized = serde.serializer().serialize("", kv);

    KeyValue<String, String> deserialized = serde.deserializer().deserialize("", serialized);

    Truth.assertThat(deserialized).isEqualTo(kv);
  }
}
