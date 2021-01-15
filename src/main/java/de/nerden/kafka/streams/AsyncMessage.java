package de.nerden.kafka.streams;

public class AsyncMessage<K, V> {
  private K key;
  private V value;
  private long offset;
  private int numFails;
  private Throwable exception;

  public AsyncMessage(K key, V value, long offset, int numFails, Throwable exception) {
    this.key = key;
    this.value = value;
    this.offset = offset;
    this.numFails = numFails;
    this.exception = exception;
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

  public Throwable getThrowable() {
    return exception;
  }

  public void addFail(Throwable t) {
    this.exception = t;
    this.numFails++;
  }
}
