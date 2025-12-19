package com.linkedin.venice.flink;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.Serializable;
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory class for creating Venice Flink sink components.
 *
 * This factory provides methods to create:
 * - {@link VeniceSinkFunction} instances for use in Flink streaming jobs
 * - {@link AbstractVeniceWriter} instances for writing to Venice topics
 *
 * The factory follows the same patterns as {@code VeniceSystemFactory} in the Samza integration,
 * adapted for Flink's streaming model.
 */
public class VeniceSinkFactory implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(VeniceSinkFactory.class);

  private final transient Time time;

  /**
   * Constructs a new VeniceSinkFactory with default time provider.
   */
  public VeniceSinkFactory() {
    this(SystemTime.INSTANCE);
  }

  /**
   * Constructs a new VeniceSinkFactory with a custom time provider.
   *
   * @param time The time provider to use
   */
  public VeniceSinkFactory(Time time) {
    this.time = time;
  }

  /**
   * Creates a new VeniceSinkFunction for the specified store.
   *
   * @param storeName The name of the Venice store to write to
   * @param controllerUrl The URL of the Venice controller
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   * @param <K> The type of the key
   * @param <V> The type of the value
   * @return A new VeniceSinkFunction instance
   */
  public <K, V> VeniceSinkFunction<K, V> createSinkFunction(
      String storeName,
      String controllerUrl,
      Version.PushType pushType) {
    return createSinkFunction(storeName, controllerUrl, pushType, Optional.empty(), Optional.empty());
  }

  /**
   * Creates a new VeniceSinkFunction for the specified store with SSL support.
   *
   * @param storeName The name of the Venice store to write to
   * @param controllerUrl The URL of the Venice controller
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   * @param sslFactory Optional SSL factory for secure communication
   * @param partitioners Optional comma-separated list of partitioner class names
   * @param <K> The type of the key
   * @param <V> The type of the value
   * @return A new VeniceSinkFunction instance
   */
  public <K, V> VeniceSinkFunction<K, V> createSinkFunction(
      String storeName,
      String controllerUrl,
      Version.PushType pushType,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    LOGGER.info(
        "Creating VeniceSinkFunction for store: {}, controllerUrl: {}, pushType: {}",
        storeName,
        controllerUrl,
        pushType);
    return new VeniceSinkFunction<>(storeName, controllerUrl, pushType, sslFactory, partitioners, time);
  }

  /**
   * Creates a VeniceWriter for writing to a Venice topic.
   *
   * @param versionCreationResponse The response from requesting a topic for writes
   * @param isChunkingEnabled Whether chunking is enabled for this store version
   * @return A new AbstractVeniceWriter instance
   */
  public AbstractVeniceWriter<byte[], byte[], byte[]> createVeniceWriter(
      VersionCreationResponse versionCreationResponse,
      boolean isChunkingEnabled) {
    return createVeniceWriter(versionCreationResponse, isChunkingEnabled, new Properties());
  }

  /**
   * Creates a VeniceWriter for writing to a Venice topic with additional configuration.
   *
   * @param versionCreationResponse The response from requesting a topic for writes
   * @param isChunkingEnabled Whether chunking is enabled for this store version
   * @param additionalConfigs Additional configuration properties
   * @return A new AbstractVeniceWriter instance
   */
  public AbstractVeniceWriter<byte[], byte[], byte[]> createVeniceWriter(
      VersionCreationResponse versionCreationResponse,
      boolean isChunkingEnabled,
      Properties additionalConfigs) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.putAll(additionalConfigs);
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, versionCreationResponse.getKafkaBootstrapServers());

    // Determine partition count based on push type
    Integer partitionCount = Version.PushType.BATCH
        .equals(getPushTypeFromTopic(versionCreationResponse.getKafkaTopic()))
        || Version.PushType.STREAM_REPROCESSING.equals(getPushTypeFromTopic(versionCreationResponse.getKafkaTopic()))
            ? versionCreationResponse.getPartitions()
            : null;

    // Create partitioner
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(versionCreationResponse.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        versionCreationResponse.getPartitionerClass(),
        new VeniceProperties(partitionerProperties));

    // Build writer options
    VeniceWriterOptions.Builder builder =
        new VeniceWriterOptions.Builder(versionCreationResponse.getKafkaTopic()).setTime(time)
            .setPartitioner(venicePartitioner)
            .setPartitionCount(partitionCount)
            .setBatchIntervalInMs(
                Long.parseLong(veniceWriterProperties.getProperty(ConfigKeys.WRITER_BATCHING_MAX_INTERVAL_MS, "0")))
            .setMaxBatchSizeInBytes(
                Integer.parseInt(
                    veniceWriterProperties.getProperty(ConfigKeys.WRITER_BATCHING_MAX_BUFFER_SIZE_IN_BYTES, "5242880")))
            .setChunkingEnabled(isChunkingEnabled);

    return constructVeniceWriter(veniceWriterProperties, builder.build());
  }

  /**
   * Constructs a VeniceWriter with the given properties and options.
   *
   * @param properties Writer properties
   * @param writerOptions Writer options
   * @return A new AbstractVeniceWriter instance
   */
  protected AbstractVeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
      Properties properties,
      VeniceWriterOptions writerOptions) {
    return new VeniceWriterFactory(properties).createAbstractVeniceWriter(writerOptions);
  }

  /**
   * Determines the push type from a topic name.
   *
   * @param topicName The topic name
   * @return The push type, or STREAM if it cannot be determined
   */
  private Version.PushType getPushTypeFromTopic(String topicName) {
    if (Version.isVersionTopic(topicName)) {
      return Version.PushType.BATCH;
    } else if (Version.isStreamReprocessingTopic(topicName)) {
      return Version.PushType.STREAM_REPROCESSING;
    } else {
      return Version.PushType.STREAM;
    }
  }
}
