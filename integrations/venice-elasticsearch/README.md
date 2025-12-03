# Venice Elasticsearch Integration

This module provides an Elasticsearch integration for Venice's Da Vinci Record Transformer (DVRT) API. It enables full-text search capabilities on Venice data by automatically mirroring store data into an Elasticsearch cluster.

## Overview

The `ElasticsearchDaVinciRecordTransformer` runs inside DaVinci clients and automatically mirrors Venice store data into Elasticsearch indices. This allows you to search your Venice data using Elasticsearch queries instead of the Venice key-value API.

When configured in your DaVinci client, it will:
- Create Elasticsearch indices that match your Venice store structure
- Keep the indices updated with Venice data changes
- Handle new Venice store versions by managing index versions
- Provide an alias that always points to the current data

## Prerequisites

- Java 11 or higher
- An Elasticsearch cluster (version 8.x recommended)
- Venice DaVinci client

## Setup

### 1. Add Dependency

Add the Venice Elasticsearch integration to your project:

```gradle
dependencies {
    implementation 'com.linkedin.venice:venice-elasticsearch:${veniceVersion}'
}
```

### 2. Configure the Transformer

Create a `DaVinciRecordTransformerConfig` with the Elasticsearch transformer:

```java
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.venice.elasticsearch.ElasticsearchDaVinciRecordTransformer;

// Elasticsearch connection parameters
String elasticsearchHost = "localhost";
int elasticsearchPort = 9200;
Set<String> fieldsToIndex = Collections.emptySet(); // Empty set indexes all fields

// Create the transformer function
DaVinciRecordTransformerFunctionalInterface transformerFunction = (
    storeName,
    storeVersion,
    keySchema,
    inputValueSchema,
    outputValueSchema,
    config) -> new ElasticsearchDaVinciRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        inputValueSchema,
        outputValueSchema,
        config,
        elasticsearchHost,
        elasticsearchPort,
        fieldsToIndex);

// Build the transformer config
DaVinciRecordTransformerConfig transformerConfig = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(transformerFunction)
    .setStoreRecordsInDaVinci(false) // Set to true if you also want local DaVinci storage
    .build();

// Configure DaVinci client
DaVinciConfig daVinciConfig = new DaVinciConfig();
daVinciConfig.setRecordTransformerConfig(transformerConfig);
```

### 3. Start the DaVinci Client

```java
DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
client.subscribeAll().get();
```

## Configuration Options

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `storeName` | String | Venice store name (provided by Venice) |
| `storeVersion` | int | Store version (provided by Venice) |
| `keySchema` | Schema | Key schema (provided by Venice) |
| `inputValueSchema` | Schema | Input value schema (provided by Venice) |
| `outputValueSchema` | Schema | Output value schema (provided by Venice) |
| `recordTransformerConfig` | DaVinciRecordTransformerConfig | Transformer config (provided by Venice) |
| `elasticsearchHost` | String | Elasticsearch host address |
| `elasticsearchPort` | int | Elasticsearch port (default: 9200) |
| `fieldsToIndex` | Set<String> | Specific fields to index (empty set for all fields) |

### DaVinciRecordTransformerConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `setStoreRecordsInDaVinci` | true | Whether to also store records in DaVinci's local RocksDB |
| `setAlwaysBootstrapFromVersionTopic` | false | Set to true if not storing in DaVinci and not backed by disk |

## How It Works

### Index Naming

The transformer creates version-specific indices following the pattern `{storeName}_v{version}`. For example:
- `my_store_v1`
- `my_store_v2`

An alias with the store name (e.g., `my_store`) always points to the current version's index.

### Version Management

When Venice pushes a new store version:

1. `onStartVersionIngestion(false)` is called for the new version
2. A new index is created (e.g., `my_store_v2`)
3. Data is ingested into the new index
4. When the new version becomes current, `onEndVersionIngestion(2)` is called
5. The alias is updated to point to the new index
6. The old index is deleted

This ensures atomic version switching with no data races.

### Document Structure

Documents in Elasticsearch have the following structure:
- Key fields are prefixed with `_key_` to avoid conflicts
- Value fields are indexed with their original names
- Nested Avro records are converted to nested JSON objects

Example document:
```json
{
  "_key_userId": "user123",
  "firstName": "John",
  "lastName": "Doe",
  "email": "john.doe@example.com"
}
```

## Querying Data

Once data is indexed, you can query it using the Elasticsearch Query DSL:

```java
// Using the Elasticsearch Java client
SearchResponse<Map> response = esClient.search(s -> s
    .index("my_store")  // Use the alias name
    .query(q -> q
        .match(m -> m
            .field("firstName")
            .query("John")
        )
    ),
    Map.class
);
```

## Testing

### Unit Tests

Run unit tests (requires Docker for Testcontainers):

```bash
./gradlew :integrations:venice-elasticsearch:test
```

### Integration Tests

Run integration tests:

```bash
./gradlew :integrations:venice-elasticsearch:integrationTest
```

## Limitations

- The transformer requires a running Elasticsearch cluster
- Large documents may need custom mapping configurations
- Complex Avro types (unions, arrays, maps) are converted to their JSON equivalents

## Troubleshooting

### Connection Issues

If you see connection errors, verify:
1. Elasticsearch is running and accessible
2. The host and port are correct
3. No firewall is blocking the connection

### Index Not Found

If queries return "index not found":
1. Ensure the DaVinci client has completed subscription
2. Check that `onStartVersionIngestion` was called
3. Verify the alias exists using `GET /_alias/{storeName}`

## License

This project is licensed under the BSD 2-Clause License - see the LICENSE file in the root directory for details.
