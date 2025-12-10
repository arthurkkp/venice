package com.linkedin.venice.flink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceRecord}.
 */
public class VeniceRecordTest {
  @Test
  public void testCreateRecordWithKeyAndValue() {
    VeniceRecord record = new VeniceRecord("key1", "value1");

    assertEquals(record.getKey(), "key1");
    assertEquals(record.getValue(), "value1");
    assertEquals(record.getLogicalTimestamp(), -1);
    assertFalse(record.isDelete());
    assertFalse(record.hasTimestamp());
  }

  @Test
  public void testCreateRecordWithKeyValueAndTimestamp() {
    VeniceRecord record = new VeniceRecord("key1", "value1", 12345L);

    assertEquals(record.getKey(), "key1");
    assertEquals(record.getValue(), "value1");
    assertEquals(record.getLogicalTimestamp(), 12345L);
    assertFalse(record.isDelete());
    assertTrue(record.hasTimestamp());
  }

  @Test
  public void testCreateDeleteRecord() {
    VeniceRecord record = VeniceRecord.delete("key1");

    assertEquals(record.getKey(), "key1");
    assertNull(record.getValue());
    assertEquals(record.getLogicalTimestamp(), -1);
    assertTrue(record.isDelete());
    assertFalse(record.hasTimestamp());
  }

  @Test
  public void testCreateDeleteRecordWithTimestamp() {
    VeniceRecord record = VeniceRecord.deleteWithTimestamp("key1", 12345L);

    assertEquals(record.getKey(), "key1");
    assertNull(record.getValue());
    assertEquals(record.getLogicalTimestamp(), 12345L);
    assertTrue(record.isDelete());
    assertTrue(record.hasTimestamp());
  }

  @Test
  public void testCreateRecordWithNullKeyThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> new VeniceRecord(null, "value1"));
  }

  @Test
  public void testCreateRecordWithNullValue() {
    VeniceRecord record = new VeniceRecord("key1", null);

    assertEquals(record.getKey(), "key1");
    assertNull(record.getValue());
    assertTrue(record.isDelete());
  }

  @Test
  public void testCreateRecordWithIntegerKey() {
    VeniceRecord record = new VeniceRecord(123, "value1");

    assertEquals(record.getKey(), 123);
    assertEquals(record.getValue(), "value1");
  }

  @Test
  public void testCreateRecordWithLongKey() {
    VeniceRecord record = new VeniceRecord(123L, "value1");

    assertEquals(record.getKey(), 123L);
    assertEquals(record.getValue(), "value1");
  }

  @Test
  public void testHasTimestampReturnsFalseForZeroTimestamp() {
    VeniceRecord record = new VeniceRecord("key1", "value1", 0);

    assertFalse(record.hasTimestamp());
  }

  @Test
  public void testHasTimestampReturnsFalseForNegativeTimestamp() {
    VeniceRecord record = new VeniceRecord("key1", "value1", -1);

    assertFalse(record.hasTimestamp());
  }

  @Test
  public void testToString() {
    VeniceRecord record = new VeniceRecord("key1", "value1", 12345L);
    String str = record.toString();

    assertTrue(str.contains("key1"));
    assertTrue(str.contains("value1"));
    assertTrue(str.contains("12345"));
  }

  @Test
  public void testToStringForDeleteRecord() {
    VeniceRecord record = VeniceRecord.delete("key1");
    String str = record.toString();

    assertTrue(str.contains("key1"));
    assertTrue(str.contains("DELETE"));
  }
}
