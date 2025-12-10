package com.linkedin.venice.flink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import java.util.Properties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link FlinkVeniceSinkFactory}.
 */
public class FlinkVeniceSinkFactoryTest {
  private FlinkVeniceSinkFactory factory;

  @BeforeMethod
  public void setUp() {
    factory = new FlinkVeniceSinkFactory();
  }

  @Test
  public void testCreateSinkWithDiscoveryUrl() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    FlinkVeniceSink sink =
        factory.createSink("http://localhost:5555", "test-store", Version.PushType.STREAM, "test-job-id", config);

    assertNotNull(sink);
    assertEquals(sink.getStoreName(), "test-store");
    assertEquals(sink.getPushType(), Version.PushType.STREAM);
  }

  @Test
  public void testCreateSinkWithD2ServiceDiscovery() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    FlinkVeniceSink sink = factory.createSink(
        "localhost:2181",
        "localhost:2181",
        "VeniceController",
        "VeniceParentController",
        "test-store",
        Version.PushType.BATCH,
        false,
        "test-job-id",
        config);

    assertNotNull(sink);
    assertEquals(sink.getStoreName(), "test-store");
    assertEquals(sink.getPushType(), Version.PushType.BATCH);
  }

  @Test
  public void testCreateSinkFromProperties() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.VENICE_STORE, "test-store");
    config.setProperty(FlinkVeniceSinkFactory.VENICE_PUSH_TYPE, "STREAM");
    config.setProperty(FlinkVeniceSinkFactory.DEPLOYMENT_ID, "test-job-id");
    config.setProperty(FlinkVeniceSinkFactory.VENICE_CONTROLLER_DISCOVERY_URL, "http://localhost:5555");
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    FlinkVeniceSink sink = factory.createSink(config);

    assertNotNull(sink);
    assertEquals(sink.getStoreName(), "test-store");
    assertEquals(sink.getPushType(), Version.PushType.STREAM);
  }

  @Test
  public void testCreateSinkWithNullDiscoveryUrlThrowsException() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    assertThrows(
        VeniceException.class,
        () -> factory.createSink(null, "test-store", Version.PushType.STREAM, "test-job-id", config));
  }

  @Test
  public void testCreateSinkWithEmptyDiscoveryUrlThrowsException() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    assertThrows(
        VeniceException.class,
        () -> factory.createSink("", "test-store", Version.PushType.STREAM, "test-job-id", config));
  }

  @Test
  public void testCreateSinkWithNullStoreNameThrowsException() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    assertThrows(
        VeniceException.class,
        () -> factory.createSink("http://localhost:5555", null, Version.PushType.STREAM, "test-job-id", config));
  }

  @Test
  public void testCreateSinkFromPropertiesWithMissingStoreNameThrowsException() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.VENICE_PUSH_TYPE, "STREAM");
    config.setProperty(FlinkVeniceSinkFactory.DEPLOYMENT_ID, "test-job-id");
    config.setProperty(FlinkVeniceSinkFactory.VENICE_CONTROLLER_DISCOVERY_URL, "http://localhost:5555");
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    assertThrows(VeniceException.class, () -> factory.createSink(config));
  }

  @Test
  public void testCreateSinkFromPropertiesWithInvalidPushTypeThrowsException() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.VENICE_STORE, "test-store");
    config.setProperty(FlinkVeniceSinkFactory.VENICE_PUSH_TYPE, "INVALID_TYPE");
    config.setProperty(FlinkVeniceSinkFactory.DEPLOYMENT_ID, "test-job-id");
    config.setProperty(FlinkVeniceSinkFactory.VENICE_CONTROLLER_DISCOVERY_URL, "http://localhost:5555");
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    assertThrows(VeniceException.class, () -> factory.createSink(config));
  }

  @Test
  public void testCreateSinkWithAggregateMode() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    FlinkVeniceSink sink = factory.createSink(
        "localhost:2181",
        "parent-localhost:2181",
        "VeniceController",
        "VeniceParentController",
        "test-store",
        Version.PushType.BATCH,
        true, // aggregate mode
        "test-job-id",
        config);

    assertNotNull(sink);
    assertEquals(sink.getStoreName(), "test-store");
    assertEquals(sink.getPushType(), Version.PushType.BATCH);
  }

  @Test
  public void testCreateSinkWithStreamReprocessingPushType() {
    Properties config = new Properties();
    config.setProperty(FlinkVeniceSinkFactory.SSL_ENABLED, "false");

    FlinkVeniceSink sink = factory
        .createSink("http://localhost:5555", "test-store", Version.PushType.STREAM_REPROCESSING, "test-job-id", config);

    assertNotNull(sink);
    assertEquals(sink.getStoreName(), "test-store");
    assertEquals(sink.getPushType(), Version.PushType.STREAM_REPROCESSING);
  }
}
