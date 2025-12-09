package com.linkedin.venice.flink;

import java.io.Serializable;


/**
 * A wrapper class that associates a value object with a timestamp.
 *
 * This class is used when writing to Venice real-time topics where
 * logical timestamps are required for conflict resolution in
 * Active-Active replication scenarios.
 *
 * @param <T> The type of the wrapped object
 */
public class VeniceObjectWithTimestamp<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final T object;
  private final long timestamp;

  /**
   * Constructs a new VeniceObjectWithTimestamp.
   *
   * @param object The value object to wrap
   * @param timestamp The logical timestamp (must be positive)
   */
  public VeniceObjectWithTimestamp(T object, long timestamp) {
    this.object = object;
    this.timestamp = timestamp;
  }

  /**
   * Gets the wrapped object.
   *
   * @return The wrapped object
   */
  public T getObject() {
    return object;
  }

  /**
   * Gets the timestamp.
   *
   * @return The logical timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }
}
