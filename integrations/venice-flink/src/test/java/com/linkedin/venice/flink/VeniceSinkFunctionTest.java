package com.linkedin.venice.flink;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSinkFunctionTest {
  private static final String TEST_STORE = "test_store";
  private static final String TEST_CONTROLLER_URL = "http://controller:5555";

  private static final String BASE_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"nameRecord\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"lastName\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"age\",\"type\":\"int\",\"default\":-1}]}";

  private static final String DERIVED_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"nameRecordWriteOpRecord\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"firstName\",\"type\":[{\"type\":\"record\",\"name\":\"NoOp\",\"fields\":[]},\"string\"],\"default\":{}},{\"name\":\"lastName\",\"type\":[\"NoOp\",\"string\"],\"default\":{}},{\"name\":\"age\",\"type\":[\"NoOp\",\"int\"],\"default\":{}}]}";

  @Test
  public void testConstructor() {
    VeniceSinkFunction<String, String> sinkFunction =
        new VeniceSinkFunction<>(TEST_STORE, TEST_CONTROLLER_URL, Version.PushType.STREAM);

    assertEquals(sinkFunction.getStoreName(), TEST_STORE);
    assertEquals(sinkFunction.getPushType(), Version.PushType.STREAM);
  }

  @Test
  public void testConstructorWithAllParameters() {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.BATCH,
        Optional.empty(),
        Optional.of("com.linkedin.venice.partitioner.DefaultVenicePartitioner"),
        mockTime);

    assertEquals(sinkFunction.getStoreName(), TEST_STORE);
    assertEquals(sinkFunction.getPushType(), Version.PushType.BATCH);
  }

  @Test
  public void testPartialUpdateConversion() {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, GenericRecord> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.BATCH,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    MultiSchemaResponse.Schema mockBaseSchema = new MultiSchemaResponse.Schema();
    mockBaseSchema.setSchemaStr(BASE_SCHEMA_STR);
    mockBaseSchema.setId(1);
    mockBaseSchema.setDerivedSchemaId(-1);

    MultiSchemaResponse.Schema mockDerivedSchema = new MultiSchemaResponse.Schema();
    mockDerivedSchema.setSchemaStr(DERIVED_SCHEMA_STR);
    mockDerivedSchema.setId(1);
    mockDerivedSchema.setDerivedSchemaId(1);

    UpdateBuilder updateBuilder = new UpdateBuilderImpl(Schema.parse(mockDerivedSchema.getSchemaStr()));
    updateBuilder.setNewFieldValue("firstName", "John");
    updateBuilder.setNewFieldValue("lastName", "Doe");
    GenericRecord partialUpdateRecord = updateBuilder.build();

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    MultiSchemaResponse blankResponse = new MultiSchemaResponse();
    blankResponse.setSchemas(new MultiSchemaResponse.Schema[] {});
    when(mockControllerClient.getAllValueAndDerivedSchema(anyString())).thenReturn(blankResponse);
    sinkFunction.setControllerClient(mockControllerClient);

    assertThrows(
        VeniceException.class,
        () -> sinkFunction.convertPartialUpdateToFullPut(new Pair<>(1, 1), partialUpdateRecord));

    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(new MultiSchemaResponse.Schema[] { mockBaseSchema, mockDerivedSchema });
    when(mockControllerClient.getAllValueAndDerivedSchema(anyString())).thenReturn(response);
    sinkFunction.refreshSchemaCache();

    GenericRecord result =
        (GenericRecord) sinkFunction.convertPartialUpdateToFullPut(new Pair<>(1, 1), partialUpdateRecord);
    assertNotNull(result);
    assertEquals(result.getSchema().toString(), mockBaseSchema.getSchemaStr());
    assertEquals(result.get("firstName").toString(), "John");
    assertEquals(result.get("lastName").toString(), "Doe");
    assertEquals(result.get("age"), -1);
  }

  @Test
  public void testControllerRequestWithRetrySuccess() {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    ControllerResponse mockResponse = mock(ControllerResponse.class);
    when(mockResponse.isError()).thenReturn(false);

    ControllerResponse result = sinkFunction.controllerRequestWithRetry(() -> mockResponse, 3);
    assertNotNull(result);
    assertEquals(result, mockResponse);
  }

  @Test
  public void testControllerRequestWithRetryFailure() throws InterruptedException {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    ControllerResponse mockResponse = mock(ControllerResponse.class);
    when(mockResponse.isError()).thenReturn(true);
    when(mockResponse.getError()).thenReturn("Test error");

    assertThrows(VeniceException.class, () -> sinkFunction.controllerRequestWithRetry(() -> mockResponse, 2));

    verify(mockTime, times(2)).sleep(anyLong());
  }

  @Test
  public void testControllerRequestWithRetryException() throws InterruptedException {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    assertThrows(VeniceException.class, () -> sinkFunction.controllerRequestWithRetry(() -> {
      throw new RuntimeException("Connection failed");
    }, 2));

    verify(mockTime, times(2)).sleep(anyLong());
  }

  @Test
  public void testSendWithStringKey() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = new SchemaResponse();
    valueSchemaResponse.setId(1);
    valueSchemaResponse.setDerivedSchemaId(-1);
    when(mockControllerClient.getValueOrDerivedSchemaId(anyString(), anyString())).thenReturn(valueSchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.complete(null);
    when(mockWriter.put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any())).thenReturn(future);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    CompletableFuture<Void> result = sinkFunction.send("testKey", "testValue");
    assertNotNull(result);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(mockWriter).put(keyCaptor.capture(), valueCaptor.capture(), anyInt(), anyLong(), any());

    assertNotNull(keyCaptor.getValue());
    assertNotNull(valueCaptor.getValue());
  }

  @Test
  public void testSendWithIntegerKey() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<Integer, String> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"int\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = new SchemaResponse();
    valueSchemaResponse.setId(1);
    valueSchemaResponse.setDerivedSchemaId(-1);
    when(mockControllerClient.getValueOrDerivedSchemaId(anyString(), anyString())).thenReturn(valueSchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.complete(null);
    when(mockWriter.put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any())).thenReturn(future);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    CompletableFuture<Void> result = sinkFunction.send(123, "testValue");
    assertNotNull(result);

    verify(mockWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
  }

  @Test
  public void testSendWithDelete() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.complete(null);
    when(mockWriter.delete(any(byte[].class), anyLong(), any())).thenReturn(future);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    CompletableFuture<Void> result = sinkFunction.send("testKey", null);
    assertNotNull(result);

    verify(mockWriter).delete(any(byte[].class), anyLong(), any());
  }

  @Test
  public void testSendWithKeySchemaValidationFailure() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"int\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    assertThrows(VeniceException.class, () -> sinkFunction.send("stringKey", "testValue"));
  }

  @Test
  public void testSendWithTimestamp() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, VeniceObjectWithTimestamp<String>> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = new SchemaResponse();
    valueSchemaResponse.setId(1);
    valueSchemaResponse.setDerivedSchemaId(-1);
    when(mockControllerClient.getValueOrDerivedSchemaId(anyString(), anyString())).thenReturn(valueSchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.complete(null);
    when(mockWriter.put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any())).thenReturn(future);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    VeniceObjectWithTimestamp<String> valueWithTimestamp = new VeniceObjectWithTimestamp<>("testValue", 12345L);
    CompletableFuture<Void> result = sinkFunction.send("testKey", valueWithTimestamp);
    assertNotNull(result);

    ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
    verify(mockWriter).put(any(byte[].class), any(byte[].class), anyInt(), timestampCaptor.capture(), any());
    assertEquals(timestampCaptor.getValue().longValue(), 12345L);
  }

  @Test
  public void testSendWithInvalidTimestamp() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, VeniceObjectWithTimestamp<String>> sinkFunction = spy(
        new VeniceSinkFunction<>(
            TEST_STORE,
            TEST_CONTROLLER_URL,
            Version.PushType.STREAM,
            Optional.empty(),
            Optional.empty(),
            mockTime));

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    sinkFunction.setVeniceWriter(mockWriter);

    doReturn("test_store_rt").when(sinkFunction).getTopicName();

    VeniceObjectWithTimestamp<String> valueWithInvalidTimestamp = new VeniceObjectWithTimestamp<>("testValue", -1L);
    assertThrows(VeniceException.class, () -> sinkFunction.send("testKey", valueWithInvalidTimestamp));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshotState() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    sinkFunction.setVeniceWriter(mockWriter);

    FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
    OperatorStateStore stateStore = mock(OperatorStateStore.class);
    ListState<Long> listState = mock(ListState.class);
    when(initContext.getOperatorStateStore()).thenReturn(stateStore);
    when(stateStore.getListState(any(ListStateDescriptor.class))).thenReturn(listState);
    when(initContext.isRestored()).thenReturn(false);

    sinkFunction.initializeState(initContext);

    FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
    when(snapshotContext.getCheckpointId()).thenReturn(1L);

    sinkFunction.snapshotState(snapshotContext);

    verify(mockWriter).flush();
    verify(listState).clear();
    verify(listState).add(0L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitializeState() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
    OperatorStateStore stateStore = mock(OperatorStateStore.class);
    ListState<Long> listState = mock(ListState.class);
    when(initContext.getOperatorStateStore()).thenReturn(stateStore);
    when(stateStore.getListState(any(ListStateDescriptor.class))).thenReturn(listState);
    when(initContext.isRestored()).thenReturn(false);

    sinkFunction.initializeState(initContext);

    verify(stateStore).getListState(any(ListStateDescriptor.class));
  }

  @Test
  public void testGetKeySchema() {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.getKeySchema();

    verify(mockControllerClient).getKeySchema(TEST_STORE);
  }

  @Test
  public void testRefreshSchemaCache() {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    ControllerClient mockControllerClient = mock(ControllerClient.class);
    MultiSchemaResponse.Schema mockSchema = new MultiSchemaResponse.Schema();
    mockSchema.setSchemaStr("\"string\"");
    mockSchema.setId(1);
    mockSchema.setDerivedSchemaId(-1);

    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(new MultiSchemaResponse.Schema[] { mockSchema });
    when(mockControllerClient.getAllValueAndDerivedSchema(anyString())).thenReturn(response);

    sinkFunction.setControllerClient(mockControllerClient);
    sinkFunction.refreshSchemaCache();

    verify(mockControllerClient).getAllValueAndDerivedSchema(TEST_STORE);
  }

  @DataProvider(name = "PushTypes")
  public Version.PushType[] pushTypes() {
    return new Version.PushType[] { Version.PushType.BATCH, Version.PushType.STREAM,
        Version.PushType.STREAM_REPROCESSING };
  }

  @Test(dataProvider = "PushTypes")
  public void testPushTypeConfiguration(Version.PushType pushType) {
    VeniceSinkFunction<String, String> sinkFunction =
        new VeniceSinkFunction<>(TEST_STORE, TEST_CONTROLLER_URL, pushType);

    assertEquals(sinkFunction.getPushType(), pushType);
  }

  @Test
  public void testClose() throws Exception {
    Time mockTime = mock(Time.class);
    VeniceSinkFunction<String, String> sinkFunction = new VeniceSinkFunction<>(
        TEST_STORE,
        TEST_CONTROLLER_URL,
        Version.PushType.STREAM,
        Optional.empty(),
        Optional.empty(),
        mockTime);

    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter = mock(AbstractVeniceWriter.class);
    ControllerClient mockControllerClient = mock(ControllerClient.class);

    sinkFunction.setVeniceWriter(mockWriter);
    sinkFunction.setControllerClient(mockControllerClient);

    sinkFunction.close();

    verify(mockWriter).flush();
    verify(mockWriter).close();
    verify(mockControllerClient).close();
  }
}
