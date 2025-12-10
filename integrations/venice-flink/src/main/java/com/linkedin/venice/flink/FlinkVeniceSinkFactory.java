package com.linkedin.venice.flink;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory class for creating {@link FlinkVeniceSink} instances.
 *
 * Flink jobs talk to either parent or child controller depending on the aggregate mode config.
 * The decision of which controller should be used is made in this factory.
 * The "Primary Controller" term is used to refer to whichever controller the Flink job should talk to.
 *
 * The primary controller should be:
 * 1. The parent controller when the Venice system is deployed in a multi-colo mode and either:
 *     a. {@link Version.PushType} is {@link Version.PushType#BATCH} or {@link Version.PushType#STREAM_REPROCESSING}; or
 *     b. The job is configured to write data in AGGREGATE mode
 * 2. The child controller when either:
 *     a. The Venice system is deployed in a single-colo mode; or
 *     b. The {@link Version.PushType} is {@link Version.PushType#STREAM} and the job is configured to write data in NON_AGGREGATE mode
 */
public class FlinkVeniceSinkFactory implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LogManager.getLogger(FlinkVeniceSinkFactory.class);

  public static final String VENICE_PUSH_TYPE = "push.type";
  public static final String VENICE_STORE = "store";
  public static final String VENICE_AGGREGATE = "aggregate";
  public static final String VENICE_CHILD_D2_ZK_HOSTS = "venice.child.d2.zk.hosts";
  public static final String VENICE_CONTROLLER_DISCOVERY_URL = "venice.controller.discovery.url";
  public static final String VENICE_ROUTER_URL = "venice.router.url";
  public static final String VENICE_PARENT_D2_ZK_HOSTS = "venice.parent.d2.zk.hosts";
  public static final String VENICE_CHILD_CONTROLLER_D2_SERVICE = "venice.child.controller.d2.service";
  public static final String VENICE_PARENT_CONTROLLER_D2_SERVICE = "venice.parent.controller.d2.service";
  public static final String LEGACY_VENICE_CHILD_CONTROLLER_D2_SERVICE = "VeniceController";
  public static final String LEGACY_VENICE_PARENT_CONTROLLER_D2_SERVICE = "VeniceParentController";
  public static final String DEPLOYMENT_ID = "deployment.id";

  private static final List<String> SSL_MANDATORY_CONFIGS = Arrays.asList(
      SSL_KEYSTORE_TYPE,
      SSL_KEYSTORE_LOCATION,
      SSL_KEY_PASSWORD,
      SSL_TRUSTSTORE_LOCATION,
      SSL_TRUSTSTORE_PASSWORD);

  public FlinkVeniceSinkFactory() {
  }

  /**
   * Creates a FlinkVeniceSink using controller discovery URL.
   *
   * @param discoveryUrl The Venice controller discovery URL
   * @param storeName The name of the Venice store to write to
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   * @param jobId A unique identifier for the Flink job
   * @param config Additional configuration properties
   * @return A configured FlinkVeniceSink instance
   */
  public FlinkVeniceSink createSink(
      String discoveryUrl,
      String storeName,
      Version.PushType pushType,
      String jobId,
      Properties config) {
    if (isEmpty(discoveryUrl)) {
      throw new VeniceException("Discovery URL cannot be null or empty");
    }
    if (isEmpty(storeName)) {
      throw new VeniceException("Store name cannot be null or empty");
    }

    String runningFabric = getRunningFabric(config);
    boolean verifyLatestProtocolPresent = getBooleanConfig(config, VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true);
    Optional<SSLFactory> sslFactory = buildSslFactory(config);
    Optional<String> partitioners = Optional.ofNullable(config.getProperty(VENICE_PARTITIONERS));
    String routerUrl = config.getProperty(VENICE_ROUTER_URL);

    LOGGER.info(
        "Creating FlinkVeniceSink for store: {}, pushType: {}, discoveryUrl: {}",
        storeName,
        pushType,
        discoveryUrl);

    return new FlinkVeniceSink(
        discoveryUrl,
        storeName,
        pushType,
        jobId,
        runningFabric,
        verifyLatestProtocolPresent,
        sslFactory,
        partitioners,
        routerUrl,
        config);
  }

  /**
   * Creates a FlinkVeniceSink using D2 service discovery.
   *
   * @param veniceChildD2ZkHost D2 ZooKeeper address for the child colo
   * @param veniceParentD2ZkHost D2 ZooKeeper address for the parent colo
   * @param childControllerD2Service D2 service name for the child controller
   * @param parentControllerD2Service D2 service name for the parent controller
   * @param storeName The name of the Venice store to write to
   * @param pushType The push type (BATCH, STREAM, or STREAM_REPROCESSING)
   * @param aggregate Whether to use aggregate mode (parent controller)
   * @param jobId A unique identifier for the Flink job
   * @param config Additional configuration properties
   * @return A configured FlinkVeniceSink instance
   */
  public FlinkVeniceSink createSink(
      String veniceChildD2ZkHost,
      String veniceParentD2ZkHost,
      String childControllerD2Service,
      String parentControllerD2Service,
      String storeName,
      Version.PushType pushType,
      boolean aggregate,
      String jobId,
      Properties config) {
    if (isEmpty(storeName)) {
      throw new VeniceException("Store name cannot be null or empty");
    }
    if (isEmpty(veniceChildD2ZkHost)) {
      throw new VeniceException("Child D2 ZK host cannot be null or empty");
    }
    if (isEmpty(veniceParentD2ZkHost)) {
      throw new VeniceException("Parent D2 ZK host cannot be null or empty");
    }

    String runningFabric = getRunningFabric(config);
    boolean verifyLatestProtocolPresent = getBooleanConfig(config, VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true);
    Optional<SSLFactory> sslFactory = buildSslFactory(config);
    Optional<String> partitioners = Optional.ofNullable(config.getProperty(VENICE_PARTITIONERS));

    String primaryControllerColoD2ZKHost;
    String primaryControllerD2Service;
    if (aggregate) {
      primaryControllerColoD2ZKHost = veniceParentD2ZkHost;
      primaryControllerD2Service =
          isEmpty(parentControllerD2Service) ? LEGACY_VENICE_PARENT_CONTROLLER_D2_SERVICE : parentControllerD2Service;
    } else {
      primaryControllerColoD2ZKHost = veniceChildD2ZkHost;
      primaryControllerD2Service =
          isEmpty(childControllerD2Service) ? LEGACY_VENICE_CHILD_CONTROLLER_D2_SERVICE : childControllerD2Service;
    }

    LOGGER.info(
        "Creating FlinkVeniceSink for store: {}, pushType: {}, aggregate: {}, primaryController: {}",
        storeName,
        pushType,
        aggregate,
        primaryControllerColoD2ZKHost);

    return new FlinkVeniceSink(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2Service,
        storeName,
        pushType,
        jobId,
        runningFabric,
        verifyLatestProtocolPresent,
        sslFactory,
        partitioners,
        config);
  }

  /**
   * Creates a FlinkVeniceSink from a Properties configuration.
   * This method extracts all necessary configuration from the properties.
   *
   * @param config Properties containing all configuration
   * @return A configured FlinkVeniceSink instance
   */
  public FlinkVeniceSink createSink(Properties config) {
    String storeName = config.getProperty(VENICE_STORE);
    String pushTypeString = config.getProperty(VENICE_PUSH_TYPE);
    String jobId = config.getProperty(DEPLOYMENT_ID);
    String discoveryUrl = config.getProperty(VENICE_CONTROLLER_DISCOVERY_URL);

    if (isEmpty(storeName)) {
      throw new VeniceException(VENICE_STORE + " must be specified in configuration");
    }

    Version.PushType pushType;
    try {
      pushType = Version.PushType.valueOf(pushTypeString);
    } catch (Exception e) {
      throw new VeniceException(
          "Cannot parse venice push type: " + pushTypeString + ". Must be one of: "
              + Arrays.stream(Version.PushType.values()).map(Enum::toString).collect(Collectors.joining(",")));
    }

    if (!isEmpty(discoveryUrl)) {
      return createSink(discoveryUrl, storeName, pushType, jobId, config);
    }

    String veniceParentZKHosts = config.getProperty(VENICE_PARENT_D2_ZK_HOSTS);
    String veniceChildZKHosts = config.getProperty(VENICE_CHILD_D2_ZK_HOSTS);
    String parentControllerD2Service = config.getProperty(VENICE_PARENT_CONTROLLER_D2_SERVICE);
    String childControllerD2Service = config.getProperty(VENICE_CHILD_CONTROLLER_D2_SERVICE);
    boolean aggregate = getBooleanConfig(config, VENICE_AGGREGATE, false);

    return createSink(
        veniceChildZKHosts,
        veniceParentZKHosts,
        childControllerD2Service,
        parentControllerD2Service,
        storeName,
        pushType,
        aggregate,
        jobId,
        config);
  }

  private String getRunningFabric(Properties config) {
    String runningFabric = config.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
    LOGGER.info("Running Fabric from config: {}", runningFabric);
    if (runningFabric == null) {
      runningFabric = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
      LOGGER.info("Running Fabric from environment: {}", runningFabric);
      if (runningFabric != null) {
        runningFabric = runningFabric.toLowerCase();
      }
    }
    if (runningFabric != null && runningFabric.contains("corp")) {
      runningFabric = NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC;
    }
    LOGGER.info("Final Running Fabric: {}", runningFabric);
    return runningFabric;
  }

  private Optional<SSLFactory> buildSslFactory(Properties config) {
    boolean sslEnabled = getBooleanConfig(config, SSL_ENABLED, true);
    if (!sslEnabled) {
      return Optional.empty();
    }

    LOGGER.info("Controller ACL is enabled.");
    String sslFactoryClassName = config.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    Properties sslProps = getSslProperties(config);
    return Optional.of(SslUtils.getSSLFactory(sslProps, sslFactoryClassName));
  }

  private Properties getSslProperties(Properties config) {
    SSL_MANDATORY_CONFIGS.forEach(requiredConfig -> {
      if (!config.containsKey(requiredConfig)) {
        throw new VeniceException("Missing a mandatory SSL config: " + requiredConfig);
      }
    });

    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED, "true");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, config.getProperty(SSL_KEYSTORE_TYPE));
    sslProperties.setProperty(SSL_KEYSTORE_LOCATION, config.getProperty(SSL_KEYSTORE_LOCATION));
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, config.getProperty(SSL_KEY_PASSWORD));
    sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, config.getProperty(SSL_TRUSTSTORE_LOCATION));
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, config.getProperty(SSL_TRUSTSTORE_PASSWORD));
    return sslProperties;
  }

  private static boolean getBooleanConfig(Properties config, String key, boolean defaultValue) {
    String value = config.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  private static boolean isEmpty(String input) {
    return (input == null) || input.isEmpty() || input.equals("null");
  }
}
