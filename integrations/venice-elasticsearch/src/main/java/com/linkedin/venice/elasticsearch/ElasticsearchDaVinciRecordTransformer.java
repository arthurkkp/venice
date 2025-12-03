package com.linkedin.venice.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.indices.update_aliases.AddAction;
import co.elastic.clients.elasticsearch.indices.update_aliases.RemoveAction;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;


/**
 * Enables full-text search of Venice data by integrating Elasticsearch into DaVinci clients.
 *
 * This transformer runs inside DaVinci clients and automatically mirrors Venice store data
 * into an Elasticsearch cluster. This allows you to search your Venice data using Elasticsearch
 * queries instead of the Venice key-value API.
 *
 * When you configure this transformer in your DaVinci client, it will:
 * - Create Elasticsearch indices that match your Venice store structure
 * - Keep the indices updated with Venice data changes
 * - Handle new Venice store versions by managing index versions
 * - Provide an alias that always points to the current data
 */
public class ElasticsearchDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LogManager.getLogger(ElasticsearchDaVinciRecordTransformer.class);
  private final String versionIndexName;
  private final String aliasName;
  private final Set<String> fieldsToIndex;
  private final RestClient restClient;
  private final ElasticsearchTransport transport;
  private final ElasticsearchClient client;

  /**
   * @param storeName the name of the Venice store
   * @param storeVersion the version of the store
   * @param keySchema the key schema
   * @param inputValueSchema the input value schema
   * @param outputValueSchema the output value schema
   * @param recordTransformerConfig the config for the record transformer
   * @param elasticsearchHost the Elasticsearch host
   * @param elasticsearchPort the Elasticsearch port
   * @param fieldsToIndex specific fields to index (leave null/empty for all fields)
   * @throws VeniceException if Elasticsearch setup fails
   */
  public ElasticsearchDaVinciRecordTransformer(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      String elasticsearchHost,
      int elasticsearchPort,
      Set<String> fieldsToIndex) {
    super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.versionIndexName = buildIndexNameWithVersion(storeVersion);
    this.aliasName = storeName;
    this.fieldsToIndex = fieldsToIndex;

    this.restClient = RestClient.builder(new HttpHost(elasticsearchHost, elasticsearchPort, "http")).build();
    this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    this.client = new ElasticsearchClient(transport);
  }

  /**
   * Note: This always returns UNCHANGED because we are not modifying the record that is persisted in DaVinci.
   */
  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  /**
   * Indexes a new/updated record in Elasticsearch when Venice receives a put event.
   */
  @Override
  public void processPut(Lazy<GenericRecord> key, Lazy<GenericRecord> value, int partitionId) {
    try {
      String documentId = extractDocumentId(key.get());
      Map<String, Object> document = convertToDocument(key.get(), value.get());

      IndexRequest<Map<String, Object>> request = IndexRequest
          .of(builder -> builder.index(versionIndexName).id(documentId).document(document).refresh(Refresh.False));

      client.index(request);
    } catch (IOException e) {
      throw new VeniceException("Failed to index document in Elasticsearch", e);
    }
  }

  /**
   * Deletes a record from Elasticsearch when Venice receives a delete event.
   */
  @Override
  public void processDelete(Lazy<GenericRecord> key, int partitionId) {
    try {
      String documentId = extractDocumentId(key.get());

      DeleteRequest request =
          DeleteRequest.of(builder -> builder.index(versionIndexName).id(documentId).refresh(Refresh.False));

      client.delete(request);
    } catch (IOException e) {
      throw new VeniceException("Failed to delete document from Elasticsearch", e);
    }
  }

  /**
   * Called when DaVinci starts ingesting a Venice store version.
   *
   * Creates the Elasticsearch index for this version if it doesn't exist.
   * If this is the current version, it also creates/updates an alias pointing to this index.
   *
   * @param isCurrentVersion true if this is the active store version
   * @throws VeniceException if index creation fails
   * @throws RuntimeException if Elasticsearch operations fail
   */
  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    try {
      boolean indexExists = client.indices().exists(ExistsRequest.of(e -> e.index(versionIndexName))).value();

      if (!indexExists) {
        LOGGER.info("Index '{}' not found, will create it from scratch", versionIndexName);
        CreateIndexRequest createRequest = CreateIndexRequest.of(builder -> builder.index(versionIndexName));
        client.indices().create(createRequest);
      } else {
        LOGGER.info("Index '{}' already exists. Will reuse.", versionIndexName);
      }

      if (isCurrentVersion) {
        updateAlias(versionIndexName);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Elasticsearch index", e);
    }
  }

  /**
   * Called when DaVinci finishes ingesting all data for the store.
   *
   * Updates the alias to point to the current version's index and
   * removes the index for the previous version that is retired.
   *
   * @param currentVersion the version that is now active
   * @throws RuntimeException if Elasticsearch operations fail
   */
  @Override
  public void onEndVersionIngestion(int currentVersion) {
    try {
      String currentVersionIndexName = buildIndexNameWithVersion(currentVersion);
      updateAlias(currentVersionIndexName);

      if (currentVersion != getStoreVersion()) {
        LOGGER.info("Deleting retired index '{}'", versionIndexName);
        boolean indexExists = client.indices().exists(ExistsRequest.of(e -> e.index(versionIndexName))).value();
        if (indexExists) {
          client.indices().delete(DeleteIndexRequest.of(d -> d.index(versionIndexName)));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to update Elasticsearch alias or delete old index", e);
    }
  }

  /**
   * Indicates this transformer works with consistent record schemas.
   *
   * @return true (requires all records to have the same structure)
   */
  public boolean useUniformInputValueSchema() {
    return true;
  }

  /**
   * Gets the Elasticsearch client.
   *
   * @return Elasticsearch client instance
   */
  public ElasticsearchClient getElasticsearchClient() {
    return client;
  }

  /**
   * Gets the version-specific index name.
   *
   * @return the index name for this version
   */
  public String getVersionIndexName() {
    return versionIndexName;
  }

  /**
   * Gets the alias name (store name).
   *
   * @return the alias name
   */
  public String getAliasName() {
    return aliasName;
  }

  /**
   * Creates a versioned index name by combining store name with version number.
   *
   * @param version the store version number
   * @return index name in format "storeName_v<version>"
   */
  public String buildIndexNameWithVersion(int version) {
    return getStoreName() + "_v" + version;
  }

  /**
   * Cleans up Elasticsearch connections and resources.
   *
   * This is called automatically when the transformer is closed.
   */
  @Override
  public void close() {
    try {
      transport.close();
      restClient.close();
    } catch (IOException e) {
      LOGGER.error("Failed to close Elasticsearch client", e);
    }
  }

  /**
   * Extracts a document ID from the key record.
   * Concatenates all key fields to create a unique document ID.
   */
  private String extractDocumentId(GenericRecord key) {
    StringBuilder idBuilder = new StringBuilder();
    Schema keySchema = key.getSchema();
    for (Schema.Field field: keySchema.getFields()) {
      Object value = key.get(field.name());
      if (idBuilder.length() > 0) {
        idBuilder.append("_");
      }
      idBuilder.append(value != null ? value.toString() : "null");
    }
    return idBuilder.toString();
  }

  /**
   * Converts key and value records to a document map for Elasticsearch.
   */
  private Map<String, Object> convertToDocument(GenericRecord key, GenericRecord value) {
    Map<String, Object> document = new HashMap<>();

    // Add key fields with prefix to avoid conflicts
    Schema keySchema = key.getSchema();
    for (Schema.Field field: keySchema.getFields()) {
      Object fieldValue = key.get(field.name());
      document.put("_key_" + field.name(), convertAvroValue(fieldValue));
    }

    // Add value fields
    if (value != null) {
      Schema valueSchema = value.getSchema();
      for (Schema.Field field: valueSchema.getFields()) {
        if (fieldsToIndex == null || fieldsToIndex.isEmpty() || fieldsToIndex.contains(field.name())) {
          Object fieldValue = value.get(field.name());
          document.put(field.name(), convertAvroValue(fieldValue));
        }
      }
    }

    return document;
  }

  /**
   * Converts an Avro value to a type suitable for Elasticsearch.
   */
  private Object convertAvroValue(Object avroValue) {
    if (avroValue == null) {
      return null;
    }
    if (avroValue instanceof CharSequence) {
      return avroValue.toString();
    }
    if (avroValue instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) avroValue;
      Map<String, Object> nestedMap = new HashMap<>();
      for (Schema.Field field: record.getSchema().getFields()) {
        nestedMap.put(field.name(), convertAvroValue(record.get(field.name())));
      }
      return nestedMap;
    }
    // For primitives (Integer, Long, Float, Double, Boolean, etc.), return as-is
    return avroValue;
  }

  /**
   * Updates the alias to point to the specified index.
   * Removes the alias from any other indices first.
   */
  private void updateAlias(String targetIndexName) throws IOException {
    // Check if alias exists and get current indices
    boolean aliasExists = false;
    try {
      GetAliasRequest getAliasRequest = GetAliasRequest.of(g -> g.name(aliasName));
      aliasExists = !client.indices().getAlias(getAliasRequest).result().isEmpty();
    } catch (Exception e) {
      // Alias doesn't exist, which is fine
      aliasExists = false;
    }

    UpdateAliasesRequest.Builder updateBuilder = new UpdateAliasesRequest.Builder();

    if (aliasExists) {
      // Remove alias from all indices first
      updateBuilder.actions(Action.of(a -> a.remove(RemoveAction.of(r -> r.index("*").alias(aliasName)))));
    }

    // Add alias to target index
    updateBuilder.actions(Action.of(a -> a.add(AddAction.of(add -> add.index(targetIndexName).alias(aliasName)))));

    client.indices().updateAliases(updateBuilder.build());
    LOGGER.info("Updated alias '{}' to point to index '{}'", aliasName, targetIndexName);
  }
}
