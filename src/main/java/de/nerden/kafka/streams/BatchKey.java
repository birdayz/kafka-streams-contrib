package de.nerden.kafka.streams;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchKey<?> batchKey = (BatchKey<?>) o;
    return offset == batchKey.offset && Objects.equals(key, batchKey.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, offset);
  }
}
