package com.linkedin.venice.flink;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Internal producer class that handles the actual communication with Venice.
 *
 * <p>This class wraps the Venice writer and controller client to provide
 * put and delete operations for the Flink sink. It follows the patterns
 * established by VeniceSystemProducer in the venice-samza integration.
 */
class VeniceFlinkSinkProducer implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(VeniceFlinkSinkProducer.class);

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

  private final String discoveryUrl;
  private final String storeName;
  private final Version.PushType pushType;
  private final String jobId;
  private final Optional<SSLFactory> sslFactory;
  private final Optional<String> partitioners;
  private final Time time;
  private final Map<String, String> additionalConfigs = new HashMap<>();

  private final VeniceConcurrentHashMap<Schema, Pair<Integer, Integer>> valueSchemaToIdsMap =
      new VeniceConcurrentHashMap<>();
  private final LoadingCache<Schema, String> canonicalSchemaStrCache =
      Caffeine.newBuilder().maximumSize(10).build(AvroCompatibilityHelper::toParsingForm);

  private Schema keySchema;
  private String canonicalKeySchemaStr;
  private ControllerClient controllerClient;
  private StoreSchemaFetcher schemaFetcher;
  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private String topicName;
  private volatile boolean isStarted = false;
  private boolean isChunkingEnabled = false;

  VeniceFlinkSinkProducer(
      String discoveryUrl,
      String storeName,
      Version.PushType pushType,
      String jobId,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    this(discoveryUrl, storeName, pushType, jobId, sslFactory, partitioners, SystemTime.INSTANCE);
  }

  VeniceFlinkSinkProducer(
      String discoveryUrl,
      String storeName,
      Version.PushType pushType,
      String jobId,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    if (discoveryUrl == null || discoveryUrl.trim().isEmpty()) {
      throw new IllegalArgumentException("Discovery URL must be provided");
    }
    this.discoveryUrl = discoveryUrl;
    this.storeName = storeName;
    this.pushType = pushType;
    this.jobId = jobId;
    this.sslFactory = sslFactory;
    this.partitioners = partitioners;
    this.time = time;
  }

  void applyAdditionalConfigs(Properties configs) {
    configs.forEach((key, value) -> additionalConfigs.put(key.toString(), value.toString()));
  }

  synchronized void start() {
    if (isStarted) {
      return;
    }
    isStarted = true;

    LOGGER.info("Starting VeniceFlinkSinkProducer for store: {}", storeName);

    this.controllerClient =
        ControllerClientFactory.discoverAndConstructControllerClient(storeName, discoveryUrl, sslFactory, 1);

    LOGGER.info("Discovery url for schema fetcher: {}", discoveryUrl);
    this.schemaFetcher = ClientFactory
        .createStoreSchemaFetcher(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(discoveryUrl));

    VersionCreationResponse versionCreationResponse = (VersionCreationResponse) controllerRequestWithRetry(
        () -> this.controllerClient.requestTopicForWrites(
            this.storeName,
            1,
            pushType,
            jobId,
            true,
            false,
            false,
            partitioners,
            Optional.empty(),
            Optional.empty(),
            false,
            -1),
        2);
    LOGGER.info("Got [store: {}] VersionCreationResponse: {}", storeName, versionCreationResponse);
    this.topicName = versionCreationResponse.getKafkaTopic();

    StoreResponse storeResponse =
        (StoreResponse) controllerRequestWithRetry(() -> this.controllerClient.getStore(storeName), 2);

    getKeySchema();
    refreshSchemaCache();

    if (pushType.isBatchOrStreamReprocessing()) {
      int versionNumber = versionCreationResponse.getVersion();
      this.isChunkingEnabled =
          storeResponse.getStore().getVersion(versionNumber).map(v -> v.isChunkingEnabled()).orElse(false);
    } else {
      this.isChunkingEnabled = false;
    }

    this.veniceWriter = getVeniceWriter(versionCreationResponse);
    LOGGER.info("VeniceFlinkSinkProducer started successfully for store: {}", storeName);
  }

  private void getKeySchema() {
    SchemaResponse keySchemaResponse =
        (SchemaResponse) controllerRequestWithRetry(() -> this.controllerClient.getKeySchema(this.storeName), 2);
    LOGGER.info("Got [store: {}] SchemaResponse for key schema: {}", storeName, keySchemaResponse);
    this.keySchema = parseSchemaFromJSONStrictValidation(keySchemaResponse.getSchemaStr());
    this.canonicalKeySchemaStr = AvroCompatibilityHelper.toParsingForm(this.keySchema);
  }

  private void refreshSchemaCache() {
    MultiSchemaResponse valueSchemaResponse = (MultiSchemaResponse) controllerRequestWithRetry(
        () -> this.controllerClient.getAllValueAndDerivedSchema(this.storeName),
        2);
    LOGGER.info("Got [store: {}] SchemaResponse for value schemas: {}", storeName, valueSchemaResponse);
    for (MultiSchemaResponse.Schema valueSchema: valueSchemaResponse.getSchemas()) {
      Schema schema = parseSchemaFromJSONLooseValidation(valueSchema.getSchemaStr());
      Pair<Integer, Integer> idPair = new Pair<>(valueSchema.getId(), valueSchema.getDerivedSchemaId());
      valueSchemaToIdsMap.put(schema, idPair);
    }
  }

  private AbstractVeniceWriter<byte[], byte[], byte[]> getVeniceWriter(VersionCreationResponse store) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.putAll(additionalConfigs);
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, store.getKafkaBootstrapServers());

    Integer partitionCount = pushType.isBatchOrStreamReprocessing() ? store.getPartitions() : null;
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(store.getPartitionerParams());
    VenicePartitioner venicePartitioner =
        PartitionUtils.getVenicePartitioner(store.getPartitionerClass(), new VeniceProperties(partitionerProperties));

    VeniceWriterOptions.Builder builder = new VeniceWriterOptions.Builder(store.getKafkaTopic()).setTime(time)
        .setPartitioner(venicePartitioner)
        .setPartitionCount(partitionCount)
        .setStoreSchemaFetcher(schemaFetcher)
        .setChunkingEnabled(isChunkingEnabled);

    Properties finalWriterConfigs = new Properties();
    finalWriterConfigs.putAll(veniceWriterProperties);
    finalWriterConfigs.putAll(additionalConfigs);
    return new VeniceWriterFactory(finalWriterConfigs).createAbstractVeniceWriter(builder.build());
  }

  CompletableFuture<Void> put(Object keyObject, Object valueObject) {
    return send(keyObject, valueObject);
  }

  CompletableFuture<Void> delete(Object keyObject) {
    return send(keyObject, null);
  }

  private CompletableFuture<Void> send(Object keyObject, Object valueObject) {
    if (!isStarted) {
      throw new VeniceException("VeniceFlinkSinkProducer has not been started. Call start() first.");
    }

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

      byte[] value = serializeObject(valueObject);

      veniceWriter.put(
          key,
          value,
          valueSchemaIdPair.getFirst(),
          logicalTimestamp,
          new CompletableFutureCallback(completableFuture));
    }
    return completableFuture;
  }

  void flush() {
    if (veniceWriter != null) {
      veniceWriter.flush();
    }
  }

  @Override
  public void close() {
    LOGGER.info("Closing VeniceFlinkSinkProducer for store: {}", storeName);
    isStarted = false;
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    LOGGER.info("VeniceFlinkSinkProducer closed for store: {}", storeName);
  }

  private ControllerResponse controllerRequestWithRetry(Supplier<ControllerResponse> supplier, int retryLimit) {
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
    throw new VeniceException("Failed to send request to Controller, error: " + errorMsg, lastException);
  }

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
}
