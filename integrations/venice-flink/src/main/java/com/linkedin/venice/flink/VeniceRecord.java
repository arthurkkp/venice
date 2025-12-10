package com.linkedin.venice.flink;

import java.io.Serializable;


/**
 * A record class that encapsulates key-value pairs for writing to Venice stores via Flink.
 * This class supports optional logical timestamps for real-time topic writes.
 */
public class VeniceRecord implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Object key;
  private final Object value;
  private final long logicalTimestamp;

  /**
   * Creates a VeniceRecord with the specified key and value.
   * The logical timestamp will be set to -1 (default).
   *
   * @param key The key object (must be Avro-compatible or a primitive type)
   * @param value The value object (must be Avro-compatible or a primitive type), or null for delete
   */
  public VeniceRecord(Object key, Object value) {
    this(key, value, -1);
  }

  /**
   * Creates a VeniceRecord with the specified key, value, and logical timestamp.
   *
   * @param key The key object (must be Avro-compatible or a primitive type)
   * @param value The value object (must be Avro-compatible or a primitive type), or null for delete
   * @param logicalTimestamp The logical timestamp for the record (used for real-time topics)
   */
  public VeniceRecord(Object key, Object value, long logicalTimestamp) {
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null");
    }
    this.key = key;
    this.value = value;
    this.logicalTimestamp = logicalTimestamp;
  }

  /**
   * Creates a delete record for the specified key.
   *
   * @param key The key to delete
   * @return A VeniceRecord representing a delete operation
   */
  public static VeniceRecord delete(Object key) {
    return new VeniceRecord(key, null);
  }

  /**
   * Creates a delete record for the specified key with a logical timestamp.
   *
   * @param key The key to delete
   * @param logicalTimestamp The logical timestamp for the delete operation
   * @return A VeniceRecord representing a delete operation
   */
  public static VeniceRecord deleteWithTimestamp(Object key, long logicalTimestamp) {
    return new VeniceRecord(key, null, logicalTimestamp);
  }

  public Object getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  public long getLogicalTimestamp() {
    return logicalTimestamp;
  }

  public boolean isDelete() {
    return value == null;
  }

  public boolean hasTimestamp() {
    return logicalTimestamp > 0;
  }

  @Override
  public String toString() {
    return "VeniceRecord{" + "key=" + key + ", value=" + (value == null ? "DELETE" : value) + ", logicalTimestamp="
        + logicalTimestamp + '}';
  }
}
