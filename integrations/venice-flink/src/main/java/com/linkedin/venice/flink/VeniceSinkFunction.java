package com.linkedin.venice.flink;

import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceSinkFunction} is a Flink sink function that writes data to Venice stores.
 *
 * This class implements Flink's {@link RichSinkFunction} and {@link CheckpointedFunction}
 * interfaces to provide exactly-once semantics with Venice's idempotent writes.
 *
 * The sink function supports the same push types as the Samza integration:
 * - BATCH: For batch data ingestion
 * - STREAM: For real-time streaming data
 * - STREAM_REPROCESSING: For reprocessing streaming data
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class VeniceSinkFunction<K, V> extends RichSinkFunction<Pair<K, V>> implements CheckpointedFunction {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(VeniceSinkFunction.class);

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final Schema BOOL_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final DatumWriter<Utf8> STRING_DATUM_WRITER = new GenericDatumWriter<>(STRING_SCHEMA);
  private static final DatumWriter<Integer> INT_DATUM_WRITER = new GenericDatumWriter<>(INT_SCHEMA);
  private static final DatumWriter<Long> LONG_DATUM_WRITER = new GenericDatumWriter<>(LONG_SCHEMA);
  private static final DatumWriter<Float> FLOAT_DATUM_WRITER = new GenericDatumWriter<>(FLOAT_SCHEMA);
  private static final DatumWriter<Double> DOUBLE_DATUM_WRITER = new GenericDatumWriter<>(DOUBLE_SCHEMA);
  private static final DatumWriter<ByteBuffer> BYTES_DATUM_WRITER = new GenericDatumWriter<>(BYTES_SCHEMA);
  private static final DatumWriter<Boolean> BOOL_DATUM_WRITER = new GenericDatumWriter<>(BOOL_SCHEMA);

  private static final WriteComputeHandlerV1 WRITE_COMPUTE_HANDLER_V1 = new WriteComputeHandlerV1();

  // Immutable configuration state (set via constructor)
  private final String storeName;
  private final String controllerUrl;
  private final Version.PushType pushType;
  private final transient Optional<SSLFactory> sslFactory;
  private final transient Optional<String> partitioners;
  private final transient Time time;

  // Schema caching
  private final VeniceConcurrentHashMap<Schema, Pair<Integer, Integer>> valueSchemaToIdsMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Pair<Integer, Integer>, Schema> valueSchemaIdsToSchemaMap =
      new VeniceConcurrentHashMap<>();
  private final transient LoadingCache<Schema, String> canonicalSchemaStrCache =
      Caffeine.newBuilder().maximumSize(10).build(AvroCompatibilityHelper::toParsingForm);

  // Mutable state (initialized in open())
  private transient Schema keySchema;
  private transient String canonicalKeySchemaStr;
  private transient ControllerClient controllerClient;
  private transient AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private transient String topicName;
  private transient String kafkaBootstrapServers;
  private transient boolean isWriteComputeEnabled;
  private transient boolean isChunkingEnabled;
  private transient boolean isStarted;

  // Checkpointing state
  private transient ListState<Long> checkpointedState;
  private transient long recordsWrittenSinceLastCheckpoint;

  /**
   * Constructs a new VeniceSinkFunction.
   *
   * @param storeName The name of the Venice store to write to
   * @param controllerUrl The URL of the Venice controller
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   */
  public VeniceSinkFunction(String storeName, String controllerUrl, Version.PushType pushType) {
    this(storeName, controllerUrl, pushType, Optional.empty(), Optional.empty(), SystemTime.INSTANCE);
  }

  /**
   * Constructs a new VeniceSinkFunction with SSL support.
   *
   * @param storeName The name of the Venice store to write to
   * @param controllerUrl The URL of the Venice controller
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   * @param sslFactory Optional SSL factory for secure communication
   * @param partitioners Optional comma-separated list of partitioner class names
   * @param time Time provider for testing
   */
  public VeniceSinkFunction(
      String storeName,
      String controllerUrl,
      Version.PushType pushType,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    this.storeName = storeName;
    this.controllerUrl = controllerUrl;
    this.pushType = pushType;
    this.sslFactory = sslFactory;
    this.partitioners = partitioners;
    this.time = time;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    if (this.isStarted) {
      return;
    }
    this.isStarted = true;

    // Initialize controller client
    this.controllerClient = new ControllerClient(storeName, controllerUrl, sslFactory);

    // Request topic for writes from Venice Controller
    String jobId = "flink-" + getRuntimeContext().getTaskNameWithSubtasks() + "-" + System.currentTimeMillis();
    VersionCreationResponse versionCreationResponse = (VersionCreationResponse) controllerRequestWithRetry(
        () -> this.controllerClient.requestTopicForWrites(
            this.storeName,
            1,
            pushType,
            jobId,
            true, // sendStartOfPush
            false, // sorted
            false, // wcEnabled
            partitioners,
            Optional.empty(),
            Optional.empty(),
            false,
            -1),
        2);
    LOGGER.info("Got [store: {}] VersionCreationResponse: {}", storeName, versionCreationResponse);
    this.topicName = versionCreationResponse.getKafkaTopic();
    this.kafkaBootstrapServers = versionCreationResponse.getKafkaBootstrapServers();

    // Get store info
    StoreResponse storeResponse =
        (StoreResponse) controllerRequestWithRetry(() -> this.controllerClient.getStore(storeName), 2);
    this.isWriteComputeEnabled = storeResponse.getStore().isWriteComputationEnabled();

    // Get key schema
    getKeySchema();

    // Load value schemas
    refreshSchemaCache();

    // Determine chunking settings
    if (pushType.isBatchOrStreamReprocessing()) {
      int versionNumber = versionCreationResponse.getVersion();
      Version version = storeResponse.getStore()
          .getVersion(versionNumber)
          .orElseThrow(
              () -> new VeniceException(
                  "Version info for version " + versionNumber + " not available in store response"));
      this.isChunkingEnabled = version.isChunkingEnabled();
    } else {
      this.isChunkingEnabled = false;
    }

    // Create Venice writer
    this.veniceWriter = new VeniceSinkFactory().createVeniceWriter(versionCreationResponse, isChunkingEnabled);

    LOGGER.info("VeniceSinkFunction opened for store: {}, topic: {}, pushType: {}", storeName, topicName, pushType);
  }

  @Override
  public void invoke(Pair<K, V> value, Context context) throws Exception {
    if (!isStarted) {
      throw new VeniceException("VeniceSinkFunction has not been started yet!");
    }

    K keyObject = value.getFirst();
    V valueObject = value.getSecond();

    send(keyObject, valueObject);
    recordsWrittenSinceLastCheckpoint++;
  }

  /**
   * Sends a key-value pair to Venice.
   *
   * @param keyObject The key object
   * @param valueObject The value object (null for delete)
   * @return A CompletableFuture that completes when the write is acknowledged
   */
  protected CompletableFuture<Void> send(Object keyObject, Object valueObject) {
    Schema keyObjectSchema = getSchemaFromObject(keyObject);
    String canonicalSchemaStr = canonicalSchemaStrCache.get(keyObjectSchema);

    if (!canonicalKeySchemaStr.equals(canonicalSchemaStr)) {
      throw new VeniceException(
          "Cannot write record to Venice store " + storeName + ", key object has schema " + canonicalSchemaStr
              + " which does not match Venice key schema " + canonicalKeySchemaStr + ".");
    }

    byte[] key = serializeObject(keyObject);
    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

    long logicalTimestamp = VeniceWriter.APP_DEFAULT_LOGICAL_TS;
    // Handle timestamp if this is a realtime topic
    if (valueObject instanceof VeniceObjectWithTimestamp && Version.isRealTimeTopic(topicName)) {
      VeniceObjectWithTimestamp objectWithTimestamp = (VeniceObjectWithTimestamp) valueObject;
      logicalTimestamp = objectWithTimestamp.getTimestamp();
      if (logicalTimestamp <= 0) {
        throw new VeniceException(
            "Timestamp specified in passed `VeniceObjectWithTimestamp` object should be positive, but received: "
                + logicalTimestamp);
      }
      valueObject = objectWithTimestamp.getObject();
    }

    if (valueObject == null) {
      veniceWriter.delete(key, logicalTimestamp, new CompletableFutureCallback(completableFuture));
    } else {
      Schema valueObjectSchema = getSchemaFromObject(valueObject);

      Pair<Integer, Integer> valueSchemaIdPair = valueSchemaToIdsMap.computeIfAbsent(valueObjectSchema, valueSchema -> {
        SchemaResponse valueSchemaResponse = (SchemaResponse) controllerRequestWithRetry(
            () -> controllerClient.getValueOrDerivedSchemaId(storeName, valueSchema.toString()),
            2);
        LOGGER.info("Got [store: {}] SchemaResponse for schema: {}", storeName, valueSchema);
        return new Pair<>(valueSchemaResponse.getId(), valueSchemaResponse.getDerivedSchemaId());
      });

      if (Version.isATopicThatIsVersioned(topicName) && valueSchemaIdPair.getSecond() != -1) {
        // Convert partial update to full put for version topics
        int baseSchemaId = valueSchemaIdPair.getFirst();
        valueObject = convertPartialUpdateToFullPut(valueSchemaIdPair, valueObject);
        valueSchemaIdPair = new Pair<>(baseSchemaId, -1);
      }

      byte[] value = serializeObject(valueObject);

      if (valueSchemaIdPair.getSecond() == -1) {
        veniceWriter.put(
            key,
            value,
            valueSchemaIdPair.getFirst(),
            logicalTimestamp,
            new CompletableFutureCallback(completableFuture));
      } else {
        if (!isWriteComputeEnabled) {
          throw new VeniceException(
              "Cannot write partial update record to Venice store " + storeName + " "
                  + "because write-compute is not enabled for it. Please contact Venice team to configure it.");
        }
        veniceWriter.update(
            key,
            value,
            valueSchemaIdPair.getFirst(),
            valueSchemaIdPair.getSecond(),
            logicalTimestamp,
            new CompletableFutureCallback(completableFuture));
      }
    }
    return completableFuture;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing VeniceSinkFunction for store: {}", storeName);
    this.isStarted = false;

    if (veniceWriter != null) {
      veniceWriter.flush();
      Utils.closeQuietlyWithErrorLogged(veniceWriter);
    }

    if (controllerClient != null) {
      Utils.closeQuietlyWithErrorLogged(controllerClient);
    }

    super.close();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // Flush pending writes before checkpoint
    if (veniceWriter != null) {
      veniceWriter.flush();
    }

    // Clear and update checkpoint state
    checkpointedState.clear();
    checkpointedState.add(recordsWrittenSinceLastCheckpoint);

    LOGGER.debug(
        "Checkpoint {} completed for store: {}, records written: {}",
        context.getCheckpointId(),
        storeName,
        recordsWrittenSinceLastCheckpoint);

    recordsWrittenSinceLastCheckpoint = 0;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("venice-sink-state", Long.class);
    checkpointedState = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored()) {
      for (Long count: checkpointedState.get()) {
        LOGGER.info("Restored checkpoint state for store: {}, previous records written: {}", storeName, count);
      }
    }

    recordsWrittenSinceLastCheckpoint = 0;
  }

  /**
   * Executes a controller request with retry logic.
   */
  protected ControllerResponse controllerRequestWithRetry(Supplier<ControllerResponse> supplier, int retryLimit) {
    String errorMsg = "";
    Exception lastException = null;
    for (int currentAttempt = 0; currentAttempt < retryLimit; currentAttempt++) {
      lastException = null;
      try {
        ControllerResponse controllerResponse = supplier.get();
        if (!controllerResponse.isError()) {
          return controllerResponse;
        } else {
          time.sleep(1000L * (currentAttempt + 1));
          errorMsg = controllerResponse.getError();
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          throw new VeniceException(e);
        }
        try {
          time.sleep(1000L * (currentAttempt + 1));
        } catch (InterruptedException ie) {
          throw new VeniceException(ie);
        }
        errorMsg = e.getMessage();
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new VeniceException(
          "Controller request failed after " + retryLimit + " attempts: " + errorMsg,
          lastException);
    }
    throw new VeniceException("Controller request failed after " + retryLimit + " attempts: " + errorMsg);
  }

  /**
   * Fetches and caches the key schema from Venice.
   */
  void getKeySchema() {
    SchemaResponse keySchemaResponse =
        (SchemaResponse) controllerRequestWithRetry(() -> this.controllerClient.getKeySchema(this.storeName), 2);
    LOGGER.info("Got [store: {}] SchemaResponse for key schema: {}", storeName, keySchemaResponse);
    this.keySchema = parseSchemaFromJSONStrictValidation(keySchemaResponse.getSchemaStr());
    this.canonicalKeySchemaStr = AvroCompatibilityHelper.toParsingForm(this.keySchema);
  }

  /**
   * Refreshes the value schema cache from Venice.
   */
  void refreshSchemaCache() {
    MultiSchemaResponse valueSchemaResponse = (MultiSchemaResponse) controllerRequestWithRetry(
        () -> this.controllerClient.getAllValueAndDerivedSchema(this.storeName),
        2);
    LOGGER.info("Got [store: {}] SchemaResponse for value schemas: {}", storeName, valueSchemaResponse);
    for (MultiSchemaResponse.Schema valueSchema: valueSchemaResponse.getSchemas()) {
      Schema schema = parseSchemaFromJSONLooseValidation(valueSchema.getSchemaStr());
      Pair<Integer, Integer> idPair = new Pair<>(valueSchema.getId(), valueSchema.getDerivedSchemaId());
      valueSchemaToIdsMap.put(schema, idPair);
      valueSchemaIdsToSchemaMap.put(idPair, schema);
    }
  }

  /**
   * Converts a partial update to a full put operation.
   */
  protected Object convertPartialUpdateToFullPut(Pair<Integer, Integer> schemaIds, Object incomingWriteValueObject) {
    Pair<Integer, Integer> baseSchemaIds = new Pair<>(schemaIds.getFirst(), -1);
    Schema baseSchema = valueSchemaIdsToSchemaMap.get(baseSchemaIds);
    if (baseSchema == null) {
      this.refreshSchemaCache();
      baseSchema = valueSchemaIdsToSchemaMap.get(baseSchemaIds);
      if (baseSchema == null) {
        throw new VeniceException(
            "Unable to find base schema with id: " + schemaIds.getFirst() + " for write compute schema with id "
                + schemaIds.getSecond());
      }
    }
    return WRITE_COMPUTE_HANDLER_V1.updateValueRecord(baseSchema, null, (GenericRecord) incomingWriteValueObject);
  }

  /**
   * Gets the Avro schema from an object.
   */
  private static Schema getSchemaFromObject(Object object) {
    if (object instanceof IndexedRecord) {
      IndexedRecord keyAvro = (IndexedRecord) object;
      return keyAvro.getSchema();
    } else if (object instanceof CharSequence) {
      return STRING_SCHEMA;
    } else if (object instanceof Integer) {
      return INT_SCHEMA;
    } else if (object instanceof Long) {
      return LONG_SCHEMA;
    } else if (object instanceof Double) {
      return DOUBLE_SCHEMA;
    } else if (object instanceof Float) {
      return FLOAT_SCHEMA;
    } else if (object instanceof byte[] || object instanceof ByteBuffer) {
      return BYTES_SCHEMA;
    } else if (object instanceof Boolean) {
      return BOOL_SCHEMA;
    } else {
      throw new VeniceException(
          "Venice Flink Sink only supports Avro objects and primitives, found object of class: "
              + object.getClass().toString());
    }
  }

  /**
   * Serializes an object to bytes.
   */
  private byte[] serializeObject(Object input) {
    if (input instanceof IndexedRecord) {
      RecordSerializer<Object> fastAvroSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(((IndexedRecord) input).getSchema());
      return fastAvroSerializer.serialize(input);
    } else if (input instanceof CharSequence) {
      return serializePrimitive(new Utf8(input.toString()), STRING_DATUM_WRITER);
    } else if (input instanceof Integer) {
      return serializePrimitive((Integer) input, INT_DATUM_WRITER);
    } else if (input instanceof Long) {
      return serializePrimitive((Long) input, LONG_DATUM_WRITER);
    } else if (input instanceof Double) {
      return serializePrimitive((Double) input, DOUBLE_DATUM_WRITER);
    } else if (input instanceof Float) {
      return serializePrimitive((Float) input, FLOAT_DATUM_WRITER);
    } else if (input instanceof ByteBuffer) {
      return serializePrimitive((ByteBuffer) input, BYTES_DATUM_WRITER);
    } else if (input instanceof byte[]) {
      return serializePrimitive(ByteBuffer.wrap((byte[]) input), BYTES_DATUM_WRITER);
    } else if (input instanceof Boolean) {
      return serializePrimitive((Boolean) input, BOOL_DATUM_WRITER);
    } else {
      throw new VeniceException(
          "Can only serialize avro objects and primitives, cannot serialize: " + input.getClass().toString());
    }
  }

  /**
   * Serializes a primitive value to bytes.
   */
  private static <T> byte[] serializePrimitive(T input, DatumWriter<T> writer) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(out);
    try {
      writer.write(input, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write input: " + input + " to binary encoder", e);
    }
    return out.toByteArray();
  }

  // Getters for testing
  public String getTopicName() {
    return topicName;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public AbstractVeniceWriter<byte[], byte[], byte[]> getVeniceWriter() {
    return veniceWriter;
  }

  public String getStoreName() {
    return storeName;
  }

  public Version.PushType getPushType() {
    return pushType;
  }

  // Package-private setters for testing
  void setControllerClient(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  void setVeniceWriter(AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter) {
    this.veniceWriter = veniceWriter;
  }
}
