package com.linkedin.venice.flink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.testng.annotations.Test;


public class VeniceObjectWithTimestampTest {
  @Test
  public void testConstructorAndGetters() {
    String testObject = "testValue";
    long testTimestamp = 12345L;

    VeniceObjectWithTimestamp<String> wrapper = new VeniceObjectWithTimestamp<>(testObject, testTimestamp);

    assertEquals(wrapper.getObject(), testObject);
    assertEquals(wrapper.getTimestamp(), testTimestamp);
  }

  @Test
  public void testWithNullObject() {
    VeniceObjectWithTimestamp<String> wrapper = new VeniceObjectWithTimestamp<>(null, 12345L);

    assertNull(wrapper.getObject());
    assertEquals(wrapper.getTimestamp(), 12345L);
  }

  @Test
  public void testWithZeroTimestamp() {
    VeniceObjectWithTimestamp<String> wrapper = new VeniceObjectWithTimestamp<>("testValue", 0L);

    assertEquals(wrapper.getObject(), "testValue");
    assertEquals(wrapper.getTimestamp(), 0L);
  }

  @Test
  public void testWithNegativeTimestamp() {
    VeniceObjectWithTimestamp<String> wrapper = new VeniceObjectWithTimestamp<>("testValue", -1L);

    assertEquals(wrapper.getObject(), "testValue");
    assertEquals(wrapper.getTimestamp(), -1L);
  }

  @Test
  public void testWithMaxTimestamp() {
    VeniceObjectWithTimestamp<String> wrapper = new VeniceObjectWithTimestamp<>("testValue", Long.MAX_VALUE);

    assertEquals(wrapper.getObject(), "testValue");
    assertEquals(wrapper.getTimestamp(), Long.MAX_VALUE);
  }

  @Test
  public void testWithIntegerObject() {
    VeniceObjectWithTimestamp<Integer> wrapper = new VeniceObjectWithTimestamp<>(42, 12345L);

    assertEquals(wrapper.getObject(), Integer.valueOf(42));
    assertEquals(wrapper.getTimestamp(), 12345L);
  }

  @Test
  public void testWithLongObject() {
    VeniceObjectWithTimestamp<Long> wrapper = new VeniceObjectWithTimestamp<>(123456789L, 12345L);

    assertEquals(wrapper.getObject(), Long.valueOf(123456789L));
    assertEquals(wrapper.getTimestamp(), 12345L);
  }

  @Test
  public void testWithDoubleObject() {
    VeniceObjectWithTimestamp<Double> wrapper = new VeniceObjectWithTimestamp<>(3.14159, 12345L);

    assertEquals(wrapper.getObject(), Double.valueOf(3.14159));
    assertEquals(wrapper.getTimestamp(), 12345L);
  }

  @Test
  public void testWithByteArrayObject() {
    byte[] testBytes = new byte[] { 1, 2, 3, 4, 5 };
    VeniceObjectWithTimestamp<byte[]> wrapper = new VeniceObjectWithTimestamp<>(testBytes, 12345L);

    assertEquals(wrapper.getObject(), testBytes);
    assertEquals(wrapper.getTimestamp(), 12345L);
  }

  @Test
  public void testSerialization() throws Exception {
    VeniceObjectWithTimestamp<String> original = new VeniceObjectWithTimestamp<>("testValue", 12345L);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(original);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    @SuppressWarnings("unchecked")
    VeniceObjectWithTimestamp<String> deserialized = (VeniceObjectWithTimestamp<String>) ois.readObject();
    ois.close();

    assertNotNull(deserialized);
    assertEquals(deserialized.getObject(), original.getObject());
    assertEquals(deserialized.getTimestamp(), original.getTimestamp());
  }

  @Test
  public void testSerializationWithNullObject() throws Exception {
    VeniceObjectWithTimestamp<String> original = new VeniceObjectWithTimestamp<>(null, 12345L);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(original);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    @SuppressWarnings("unchecked")
    VeniceObjectWithTimestamp<String> deserialized = (VeniceObjectWithTimestamp<String>) ois.readObject();
    ois.close();

    assertNotNull(deserialized);
    assertNull(deserialized.getObject());
    assertEquals(deserialized.getTimestamp(), original.getTimestamp());
  }

  @Test
  public void testMultipleInstances() {
    VeniceObjectWithTimestamp<String> wrapper1 = new VeniceObjectWithTimestamp<>("value1", 100L);
    VeniceObjectWithTimestamp<String> wrapper2 = new VeniceObjectWithTimestamp<>("value2", 200L);
    VeniceObjectWithTimestamp<String> wrapper3 = new VeniceObjectWithTimestamp<>("value3", 300L);

    assertEquals(wrapper1.getObject(), "value1");
    assertEquals(wrapper1.getTimestamp(), 100L);

    assertEquals(wrapper2.getObject(), "value2");
    assertEquals(wrapper2.getTimestamp(), 200L);

    assertEquals(wrapper3.getObject(), "value3");
    assertEquals(wrapper3.getTimestamp(), 300L);
  }
}
