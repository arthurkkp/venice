package com.linkedin.venice.elasticsearch;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ElasticsearchDaVinciRecordTransformerTest {
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  static final String storeName = "test_store";
  private final Set<String> fieldsToIndex = Collections.emptySet();

  private static ElasticsearchContainer elasticsearchContainer;
  private static String elasticsearchHost;
  private static int elasticsearchPort;

  @BeforeClass
  public static void setUpClass() {
    elasticsearchContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.11.0")
        .withEnv("xpack.security.enabled", "false");
    elasticsearchContainer.start();
    elasticsearchHost = elasticsearchContainer.getHost();
    elasticsearchPort = elasticsearchContainer.getMappedPort(9200);
  }

  @AfterClass
  public static void tearDownClass() {
    if (elasticsearchContainer != null) {
      elasticsearchContainer.stop();
    }
  }

  @Test
  public void testRecordTransformer() throws Exception {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    try (ElasticsearchDaVinciRecordTransformer recordTransformer = new ElasticsearchDaVinciRecordTransformer(
        storeName,
        storeVersion,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex)) {
      assertTrue(recordTransformer.useUniformInputValueSchema());

      Schema keySchema = recordTransformer.getKeySchema();
      assertEquals(keySchema.getType(), Schema.Type.RECORD);

      Schema outputValueSchema = recordTransformer.getOutputValueSchema();
      assertEquals(outputValueSchema.getType(), Schema.Type.RECORD);

      recordTransformer.onStartVersionIngestion(true);

      GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
      keyRecord.put("key", "key1");
      Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

      GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      valueRecord.put("firstName", "Duck");
      valueRecord.put("lastName", "Goose");
      Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord);

      DaVinciRecordTransformerResult<GenericRecord> transformerResult =
          recordTransformer.transform(lazyKey, lazyValue, partitionId);
      recordTransformer.processPut(lazyKey, lazyValue, partitionId);
      assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.UNCHANGED);
      // Result will be empty when it's UNCHANGED
      assertNull(transformerResult.getValue());
      assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue, partitionId));

      // Refresh the index to make the document searchable
      ElasticsearchClient client = recordTransformer.getElasticsearchClient();
      client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformer.getVersionIndexName())));

      // Verify document was indexed
      GetResponse<Map> getResponse =
          client.get(GetRequest.of(g -> g.index(recordTransformer.getVersionIndexName()).id("key1")), Map.class);
      assertTrue(getResponse.found());
      Map<String, Object> source = getResponse.source();
      assertEquals(source.get("firstName"), "Duck");
      assertEquals(source.get("lastName"), "Goose");

      recordTransformer.processDelete(lazyKey, partitionId);
      client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformer.getVersionIndexName())));

      // Verify document was deleted
      GetResponse<Map> getResponseAfterDelete =
          client.get(GetRequest.of(g -> g.index(recordTransformer.getVersionIndexName()).id("key1")), Map.class);
      assertFalse(getResponseAfterDelete.found());

      assertFalse(recordTransformer.getStoreRecordsInDaVinci());

      int classHash = recordTransformer.getClassHash();

      DaVinciRecordTransformerUtility<GenericRecord, GenericRecord> recordTransformerUtility =
          recordTransformer.getRecordTransformerUtility();
      OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);

      assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));

      offsetRecord.setRecordTransformerClassHash(classHash);

      assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
    }
  }

  @Test
  public void testVersionSwap() throws Exception {
    String testStoreName = "version_swap_store";

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    ElasticsearchDaVinciRecordTransformer recordTransformer_v1 = new ElasticsearchDaVinciRecordTransformer(
        testStoreName,
        1,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex);
    ElasticsearchDaVinciRecordTransformer recordTransformer_v2 = new ElasticsearchDaVinciRecordTransformer(
        testStoreName,
        2,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex);

    ElasticsearchClient client = recordTransformer_v1.getElasticsearchClient();

    recordTransformer_v1.onStartVersionIngestion(true);
    recordTransformer_v2.onStartVersionIngestion(false);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key1");
    Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

    GenericRecord valueRecord_v1 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v1.put("firstName", "Duck");
    valueRecord_v1.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord_v1);
    recordTransformer_v1.processPut(lazyKey, lazyValue, partitionId);

    GenericRecord valueRecord_v2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v2.put("firstName", "Goose");
    valueRecord_v2.put("lastName", "Duck");
    lazyValue = Lazy.of(() -> valueRecord_v2);
    recordTransformer_v2.processPut(lazyKey, lazyValue, partitionId);

    // Refresh indices
    client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformer_v1.getVersionIndexName())));
    client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformer_v2.getVersionIndexName())));

    // Verify v1 data via alias
    SearchResponse<Map> searchResponse =
        client.search(SearchRequest.of(s -> s.index(testStoreName).size(10)), Map.class);
    assertEquals(searchResponse.hits().total().value(), 1L);
    Map<String, Object> hit = searchResponse.hits().hits().get(0).source();
    assertEquals(hit.get("firstName"), "Duck");
    assertEquals(hit.get("lastName"), "Goose");

    // Swap here
    recordTransformer_v1.onEndVersionIngestion(2);

    // Verify alias now points to v2 data
    searchResponse = client.search(SearchRequest.of(s -> s.index(testStoreName).size(10)), Map.class);
    assertEquals(searchResponse.hits().total().value(), 1L);
    hit = searchResponse.hits().hits().get(0).source();
    assertEquals(hit.get("firstName"), "Goose");
    assertEquals(hit.get("lastName"), "Duck");

    // Verify v1 index was deleted
    boolean v1IndexExists =
        client.indices().exists(ExistsRequest.of(e -> e.index(recordTransformer_v1.getVersionIndexName()))).value();
    assertFalse(v1IndexExists);

    recordTransformer_v1.close();
    recordTransformer_v2.close();
  }

  @Test
  public void testTwoIndicesConcurrently() throws Exception {
    String store1 = "es_store1";
    String store2 = "es_store2";

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    ElasticsearchDaVinciRecordTransformer recordTransformerForStore1 = new ElasticsearchDaVinciRecordTransformer(
        store1,
        1,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex);
    ElasticsearchDaVinciRecordTransformer recordTransformerForStore2 = new ElasticsearchDaVinciRecordTransformer(
        store2,
        1,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex);

    ElasticsearchClient client = recordTransformerForStore1.getElasticsearchClient();

    recordTransformerForStore1.onStartVersionIngestion(true);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key1");
    Lazy<GenericRecord> lazyKeyForStore1 = Lazy.of(() -> keyRecord);

    GenericRecord valueRecordForStore1 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecordForStore1.put("firstName", "Duck");
    valueRecordForStore1.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValueForStore1 = Lazy.of(() -> valueRecordForStore1);
    recordTransformerForStore1.processPut(lazyKeyForStore1, lazyValueForStore1, partitionId);

    recordTransformerForStore2.onStartVersionIngestion(true);

    GenericRecord keyRecordForStore2 = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecordForStore2.put("key", "key2");
    Lazy<GenericRecord> lazyKeyForStore2 = Lazy.of(() -> keyRecordForStore2);

    GenericRecord valueRecordForStore2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecordForStore2.put("firstName", "Swan");
    valueRecordForStore2.put("lastName", "Lake");
    Lazy<GenericRecord> lazyValueForStore2 = Lazy.of(() -> valueRecordForStore2);
    recordTransformerForStore2.processPut(lazyKeyForStore2, lazyValueForStore2, partitionId);

    // Refresh indices
    client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformerForStore1.getVersionIndexName())));
    client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformerForStore2.getVersionIndexName())));

    // Verify store1 data
    SearchResponse<Map> searchResponse1 = client.search(SearchRequest.of(s -> s.index(store1).size(10)), Map.class);
    assertEquals(searchResponse1.hits().total().value(), 1L);
    Map<String, Object> hit1 = searchResponse1.hits().hits().get(0).source();
    assertEquals(hit1.get("firstName"), "Duck");
    assertEquals(hit1.get("lastName"), "Goose");

    // Verify store2 data
    SearchResponse<Map> searchResponse2 = client.search(SearchRequest.of(s -> s.index(store2).size(10)), Map.class);
    assertEquals(searchResponse2.hits().total().value(), 1L);
    Map<String, Object> hit2 = searchResponse2.hits().hits().get(0).source();
    assertEquals(hit2.get("firstName"), "Swan");
    assertEquals(hit2.get("lastName"), "Lake");

    // Delete from store2
    recordTransformerForStore2.processDelete(lazyKeyForStore2, partitionId);
    client.indices().refresh(RefreshRequest.of(r -> r.index(recordTransformerForStore2.getVersionIndexName())));

    // Verify store2 is empty
    searchResponse2 = client.search(SearchRequest.of(s -> s.index(store2).size(10)), Map.class);
    assertEquals(searchResponse2.hits().total().value(), 0L);

    // Verify store1 still has data
    searchResponse1 = client.search(SearchRequest.of(s -> s.index(store1).size(10)), Map.class);
    assertEquals(searchResponse1.hits().total().value(), 1L);

    recordTransformerForStore1.close();
    recordTransformerForStore2.close();
  }
}
