package com.linkedin.venice.flink;

import java.io.Serializable;


/**
 * A record wrapper for key-value pairs to be written to Venice via Flink.
 *
 * <p>This class encapsulates a key-value pair for writing to Venice stores.
 * A null value indicates a delete operation for the given key.
 *
 * <p>Example usage:
 * <pre>{@code
 * // For a put operation
 * VeniceFlinkRecord<String, MyValue> putRecord = new VeniceFlinkRecord<>("key1", myValue);
 *
 * // For a delete operation
 * VeniceFlinkRecord<String, MyValue> deleteRecord = new VeniceFlinkRecord<>("key1", null);
 * }</pre>
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class VeniceFlinkRecord<K, V> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final K key;
  private final V value;

  public VeniceFlinkRecord(K key, V value) {
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null");
    }
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public boolean isDelete() {
    return value == null;
  }

  @Override
  public String toString() {
    return "VeniceFlinkRecord{key=" + key + ", value=" + (value == null ? "DELETE" : value) + "}";
  }
}
