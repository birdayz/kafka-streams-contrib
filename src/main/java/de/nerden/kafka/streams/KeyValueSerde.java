package de.nerden.kafka.streams;

import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerde<K, V> implements Serde<KeyValue<K, V>> {

	private class KeyValueSerializer implements Serializer<KeyValue<K, V>> {

		private final Serializer<K> keySerializer;
		private final Serializer<V> valueSerializer;

		public KeyValueSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public byte[] serialize(String topic, KeyValue<K, V> data) {
			byte[] key = this.keySerializer.serialize("", data.key);
			byte[] value = this.valueSerializer.serialize("", data.value);

			return ByteBuffer.allocate(key.length + value.length + 4 + 4).putInt(key.length)
					.put(key).putInt(value.length).put(value).array();
		}
	}

	@Override
	public Serializer<KeyValue<K, V>> serializer() {
		return null;
	}

	@Override
	public Deserializer<KeyValue<K, V>> deserializer() {
		return null;
	}
}
