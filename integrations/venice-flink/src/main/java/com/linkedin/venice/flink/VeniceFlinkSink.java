package com.linkedin.venice.flink;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A Flink sink connector for writing data to Venice stores.
 *
 * This sink wraps the {@link VeniceFlinkSinkProducer} to enable streaming data writes
 * from Apache Flink to Venice stores. It extends {@link RichSinkFunction} to leverage
 * Flink's lifecycle management for proper initialization and cleanup of Venice resources.
 *
 * <p>The sink supports writing Avro records and primitive types to Venice stores using
 * the STREAM push type for real-time data ingestion.
 *
 * <p>Example usage:
 * <pre>{@code
 * DataStream<MyRecord> stream = ...;
 * stream.addSink(VeniceFlinkSink.<MyRecord>builder()
 *     .setDiscoveryUrl("http://venice-controller:5555")
 *     .setStoreName("my-store")
 *     .setJobId("my-flink-job")
 *     .build());
 * }</pre>
 *
 * @param <T> The type of elements to be written to Venice
 */
public class VeniceFlinkSink<T> extends RichSinkFunction<T> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(VeniceFlinkSink.class);

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

  private static final long DEFAULT_WRITE_TIMEOUT_MS = 30000L;

  private final String discoveryUrl;
  private final String storeName;
  private final String jobId;
  private final String pushTypeString;
  private final String partitioners;
  private final Properties additionalConfigs;
  private final long writeTimeoutMs;

  private transient Optional<SSLFactory> sslFactory;

  private transient VeniceFlinkSinkProducer producer;

  private VeniceFlinkSink(Builder<T> builder) {
    this.discoveryUrl = builder.discoveryUrl;
    this.storeName = builder.storeName;
    this.jobId = builder.jobId;
    this.pushTypeString = builder.pushType.name();
    this.sslFactory = builder.sslFactory;
    this.partitioners = builder.partitioners.orElse(null);
    this.additionalConfigs = builder.additionalConfigs;
    this.writeTimeoutMs = builder.writeTimeoutMs;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    LOGGER.info("Opening VeniceFlinkSink for store: {}", storeName);

    if (sslFactory == null) {
      sslFactory = Optional.empty();
    }

    Version.PushType pushType = Version.PushType.valueOf(pushTypeString);
    Optional<String> partitionersOpt = Optional.ofNullable(partitioners);
    producer = new VeniceFlinkSinkProducer(discoveryUrl, storeName, pushType, jobId, sslFactory, partitionersOpt);

    if (!additionalConfigs.isEmpty()) {
      producer.applyAdditionalConfigs(additionalConfigs);
    }

    producer.start();
    LOGGER.info("VeniceFlinkSink opened successfully for store: {}", storeName);
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    if (producer == null) {
      throw new VeniceException("VeniceFlinkSink has not been opened. Call open() before invoke().");
    }

    if (value == null) {
      LOGGER.warn("Received null value, skipping write");
      return;
    }

    if (value instanceof VeniceFlinkRecord) {
      VeniceFlinkRecord record = (VeniceFlinkRecord) value;
      Object key = record.getKey();
      Object recordValue = record.getValue();

      CompletableFuture<Void> future;
      if (recordValue == null) {
        future = producer.delete(key);
      } else {
        future = producer.put(key, recordValue);
      }

      try {
        future.get(writeTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        throw new VeniceException("Write to Venice timed out after " + writeTimeoutMs + "ms", e);
      } catch (ExecutionException e) {
        throw new VeniceException("Failed to write to Venice store: " + storeName, e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Write to Venice was interrupted", e);
      }
    } else {
      throw new VeniceException(
          "VeniceFlinkSink expects VeniceFlinkRecord type, but received: " + value.getClass().getName()
              + ". Please wrap your key-value pairs in VeniceFlinkRecord.");
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing VeniceFlinkSink for store: {}", storeName);
    if (producer != null) {
      try {
        producer.flush();
        producer.close();
      } catch (Exception e) {
        LOGGER.error("Error closing VeniceFlinkSink producer for store: {}", storeName, e);
        throw e;
      } finally {
        producer = null;
      }
    }
    super.close();
    LOGGER.info("VeniceFlinkSink closed successfully for store: {}", storeName);
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {
    private String discoveryUrl;
    private String storeName;
    private String jobId;
    private Version.PushType pushType = Version.PushType.STREAM;
    private Optional<SSLFactory> sslFactory = Optional.empty();
    private Optional<String> partitioners = Optional.empty();
    private Properties additionalConfigs = new Properties();
    private long writeTimeoutMs = DEFAULT_WRITE_TIMEOUT_MS;

    public Builder<T> setDiscoveryUrl(String discoveryUrl) {
      this.discoveryUrl = discoveryUrl;
      return this;
    }

    public Builder<T> setStoreName(String storeName) {
      this.storeName = storeName;
      return this;
    }

    public Builder<T> setJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder<T> setPushType(Version.PushType pushType) {
      this.pushType = pushType;
      return this;
    }

    public Builder<T> setSslFactory(SSLFactory sslFactory) {
      this.sslFactory = Optional.ofNullable(sslFactory);
      return this;
    }

    public Builder<T> setPartitioners(String partitioners) {
      this.partitioners = Optional.ofNullable(partitioners);
      return this;
    }

    public Builder<T> setAdditionalConfigs(Properties additionalConfigs) {
      this.additionalConfigs = additionalConfigs;
      return this;
    }

    public Builder<T> setWriteTimeoutMs(long writeTimeoutMs) {
      this.writeTimeoutMs = writeTimeoutMs;
      return this;
    }

    public VeniceFlinkSink<T> build() {
      if (discoveryUrl == null || discoveryUrl.isEmpty()) {
        throw new IllegalArgumentException("Discovery URL must be provided");
      }
      if (storeName == null || storeName.isEmpty()) {
        throw new IllegalArgumentException("Store name must be provided");
      }
      if (jobId == null || jobId.isEmpty()) {
        throw new IllegalArgumentException("Job ID must be provided");
      }
      return new VeniceFlinkSink<>(this);
    }
  }
}
