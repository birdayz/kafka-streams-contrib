package de.nerden.kafka.streams;

public class RetryMessage<K, V> {
  private K key;
  private V value;
  private long offset;
  private int numFails;

  public RetryMessage(K key, V value, long offset, int numFails) {
    this.key = key;
    this.value = value;
    this.offset = offset;
    this.numFails = numFails;
  }

  public V getValue() {
    return value;
  }

  public long getOffset() {
    return offset;
  }

  public int getNumFails() {
    return numFails;
  }

  public K getKey() {
    return key;
  }
}
