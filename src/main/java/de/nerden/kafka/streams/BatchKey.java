package de.nerden.kafka.streams;

public class BatchKey<K> {
  private K key;
  private long offset;

  public BatchKey(K key, long offset) {
    this.key = key;
    this.offset = offset;
  }

  public K getKey() {
    return key;
  }

  public long getOffset() {
    return offset;
  }
}
