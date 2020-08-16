package de.nerden.kafka.streams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class BatchingProcessor<K, V> implements Processor<K, V> {

	private KeyValueStore<Long, V> store;
	private ProcessorContext context;

	@Override
	@SuppressWarnings("unchecked")
	public void init(final ProcessorContext context) {
		store = (KeyValueStore<Long, V>) context.getStateStore("batch");
		this.context = context;
		this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME,
				timestamp -> {
					forwardBatch();
				});
	}

	@Override
	public void process(final K key, final V value) {
		this.store.put(context.offset(), value);
	}

	private void forwardBatch() {
		final KeyValueIterator<Long, V> all = this.store.all();

		List<V> res = new ArrayList<>();

		all.forEachRemaining(kv -> res.add(kv.value));

		if (!res.isEmpty()) {
			context.forward(null, res);
		}
	}

	@Override
	public void close() {
	}
}
