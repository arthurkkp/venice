package com.linkedin.venice.flink;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Optional;
import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSinkFactoryTest {
  private static final String TEST_STORE = "test_store";
  private static final String TEST_CONTROLLER_URL = "http://controller:5555";
  private static final String TEST_KAFKA_BOOTSTRAP_SERVERS = "kafka.test:9092";

  @Test
  public void testDefaultConstructor() {
    VeniceSinkFactory factory = new VeniceSinkFactory();
    assertNotNull(factory);
  }

  @Test
  public void testConstructorWithTime() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = new VeniceSinkFactory(mockTime);
    assertNotNull(factory);
  }

  @Test
  public void testCreateSinkFunctionBasic() {
    VeniceSinkFactory factory = new VeniceSinkFactory();

    VeniceSinkFunction<String, String> sinkFunction =
        factory.createSinkFunction(TEST_STORE, TEST_CONTROLLER_URL, Version.PushType.STREAM);

    assertNotNull(sinkFunction);
    assertEquals(sinkFunction.getStoreName(), TEST_STORE);
    assertEquals(sinkFunction.getPushType(), Version.PushType.STREAM);
  }

  @Test
  public void testCreateSinkFunctionWithSSL() {
    VeniceSinkFactory factory = new VeniceSinkFactory();

    VeniceSinkFunction<String, String> sinkFunction = factory.createSinkFunction(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.BATCH,
        Optional.empty(),
        Optional.of("com.linkedin.venice.partitioner.DefaultVenicePartitioner"));

    assertNotNull(sinkFunction);
    assertEquals(sinkFunction.getStoreName(), TEST_STORE);
    assertEquals(sinkFunction.getPushType(), Version.PushType.BATCH);
  }

  @DataProvider(name = "PushTypes")
  public Version.PushType[] pushTypes() {
    return new Version.PushType[] { Version.PushType.BATCH, Version.PushType.STREAM,
        Version.PushType.STREAM_REPROCESSING };
  }

  @Test(dataProvider = "PushTypes")
  public void testCreateSinkFunctionWithDifferentPushTypes(Version.PushType pushType) {
    VeniceSinkFactory factory = new VeniceSinkFactory();

    VeniceSinkFunction<String, String> sinkFunction =
        factory.createSinkFunction(TEST_STORE, TEST_CONTROLLER_URL, pushType);

    assertNotNull(sinkFunction);
    assertEquals(sinkFunction.getPushType(), pushType);
  }

  @Test
  public void testCreateVeniceWriterForBatchPush() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);

    doReturn(mockWriter).when(factory).constructVeniceWriter(propertiesCaptor.capture(), optionsCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(4);
    versionCreationResponse.setKafkaTopic("test_store_v1");

    AbstractVeniceWriter<byte[], byte[], byte[]> result = factory.createVeniceWriter(versionCreationResponse, false);

    assertNotNull(result);
    assertEquals(result, mockWriter);

    Properties capturedProperties = propertiesCaptor.getValue();
    assertEquals(capturedProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS), TEST_KAFKA_BOOTSTRAP_SERVERS);

    VeniceWriterOptions capturedOptions = optionsCaptor.getValue();
    assertEquals(capturedOptions.getTopicName(), "test_store_v1");
    assertNotNull(capturedOptions.getPartitionCount());
    assertEquals((int) capturedOptions.getPartitionCount(), 4);
  }

  @Test
  public void testCreateVeniceWriterForStreamPush() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);

    doReturn(mockWriter).when(factory).constructVeniceWriter(propertiesCaptor.capture(), optionsCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(4);
    versionCreationResponse.setKafkaTopic("test_store_rt");

    AbstractVeniceWriter<byte[], byte[], byte[]> result = factory.createVeniceWriter(versionCreationResponse, false);

    assertNotNull(result);

    VeniceWriterOptions capturedOptions = optionsCaptor.getValue();
    assertEquals(capturedOptions.getTopicName(), "test_store_rt");
    assertNull(capturedOptions.getPartitionCount());
  }

  @Test
  public void testCreateVeniceWriterForStreamReprocessing() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);

    doReturn(mockWriter).when(factory).constructVeniceWriter(propertiesCaptor.capture(), optionsCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(8);
    versionCreationResponse.setKafkaTopic("test_store_v1_sr");

    AbstractVeniceWriter<byte[], byte[], byte[]> result = factory.createVeniceWriter(versionCreationResponse, false);

    assertNotNull(result);

    VeniceWriterOptions capturedOptions = optionsCaptor.getValue();
    assertEquals(capturedOptions.getTopicName(), "test_store_v1_sr");
    assertNotNull(capturedOptions.getPartitionCount());
    assertEquals((int) capturedOptions.getPartitionCount(), 8);
  }

  @Test
  public void testCreateVeniceWriterWithChunking() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);

    doReturn(mockWriter).when(factory).constructVeniceWriter(any(Properties.class), optionsCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(4);
    versionCreationResponse.setKafkaTopic("test_store_v1");

    factory.createVeniceWriter(versionCreationResponse, true);

    VeniceWriterOptions capturedOptions = optionsCaptor.getValue();
    assertEquals(capturedOptions.isChunkingEnabled(), true);
  }

  @Test
  public void testCreateVeniceWriterWithAdditionalConfigs() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);

    doReturn(mockWriter).when(factory)
        .constructVeniceWriter(propertiesCaptor.capture(), any(VeniceWriterOptions.class));

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(4);
    versionCreationResponse.setKafkaTopic("test_store_v1");

    Properties additionalConfigs = new Properties();
    additionalConfigs.put("custom.config", "custom.value");

    factory.createVeniceWriter(versionCreationResponse, false, additionalConfigs);

    Properties capturedProperties = propertiesCaptor.getValue();
    assertEquals(capturedProperties.getProperty("custom.config"), "custom.value");
    assertEquals(capturedProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS), TEST_KAFKA_BOOTSTRAP_SERVERS);
  }

  @Test
  public void testCreateVeniceWriterWithPartitionerParams() {
    Time mockTime = mock(Time.class);
    VeniceSinkFactory factory = spy(new VeniceSinkFactory(mockTime));

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);

    doReturn(mockWriter).when(factory).constructVeniceWriter(any(Properties.class), any(VeniceWriterOptions.class));

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers(TEST_KAFKA_BOOTSTRAP_SERVERS);
    versionCreationResponse.setPartitions(4);
    versionCreationResponse.setKafkaTopic("test_store_v1");
    versionCreationResponse.setPartitionerClass("com.linkedin.venice.partitioner.DefaultVenicePartitioner");

    AbstractVeniceWriter<byte[], byte[], byte[]> result = factory.createVeniceWriter(versionCreationResponse, false);

    assertNotNull(result);
  }
}
