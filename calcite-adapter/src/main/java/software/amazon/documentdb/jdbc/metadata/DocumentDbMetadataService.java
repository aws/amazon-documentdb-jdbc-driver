/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.metadata;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaSecurityException;
import software.amazon.documentdb.jdbc.persist.SchemaReader;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NEW;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NONE;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

/**
 * A service for retrieving DocumentDB database metadata.
 */
public class DocumentDbMetadataService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMetadataService.class);
    private static final Map<String, DocumentDbSchemaTable> TABLE_MAP = new ConcurrentHashMap<>();

    /**
     * Gets the latest or a new {@link DocumentDbDatabaseSchemaMetadata} instance based on the
     * schemaName and properties. It uses a value of {@link DocumentDbDatabaseSchemaMetadata#VERSION_LATEST_OR_NEW}
     * for the version to indicate to get the latest or create a new instance if none exists.
     *
     * @param properties the connection properties.
     * @param schemaName the client ID.
     * @param client the {@link MongoClient} client.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final MongoClient client) throws SQLException {
        return get(properties, schemaName, VERSION_LATEST_OR_NEW, client);
    }

    /**
     * Gets an existing {@link DocumentDbDatabaseSchemaMetadata} instance based on the clientId and
     * version.
     *
    /**
     * Gets an existing {@link DocumentDbDatabaseSchemaMetadata} instance based on the clientId and
     * version.
     *
     * @param properties the connection properties.
     * @param schemaName the client ID.
     * @param schemaVersion the version of the metadata. A version number of
     *                {@link DocumentDbDatabaseSchemaMetadata#VERSION_LATEST_OR_NEW} indicates to get the latest
     *                or create a new instance.
     * @param client the {@link MongoClient} client.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance if the clientId and version exist,
     * {@code null} otherwise.
     */
    @Nullable
    public static DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final MongoClient client) throws SQLException {
        final Instant beginRetrieval = Instant.now();
        final Map<String, DocumentDbSchemaTable> tableMap = new LinkedHashMap<>();
        final DocumentDbSchema schema;
        // ASSUMPTION: Negative versions handle special cases
        final int lookupVersion = Math.max(schemaVersion, VERSION_LATEST_OR_NEW);
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties, client);
        try {
            // Get the latest or specific version, might not exist
            schema = schemaReader.read(schemaName, lookupVersion);

            switch (schemaVersion) {
                case VERSION_LATEST_OR_NEW:
                    // If latest exist, return it.
                    if (schema != null) {
                        LOGGER.info(
                                String.format("Successfully retrieved metadata schema %s in %d ms.",
                                        schemaName, Instant.now().toEpochMilli()
                                                - beginRetrieval.toEpochMilli()));
                        return schema;
                    }
                    LOGGER.info(String.format(
                            "Existing metadata not found for schema %s, will generate new metadata instead for database %s.",
                            schemaName, properties.getDatabase()));
                    return getNewDatabaseMetadata(properties, schemaName, 1, tableMap, client);
                case VERSION_NEW:
                    final int newVersionNumber = schema != null ? schema.getSchemaVersion() + 1 : 1;
                    return getNewDatabaseMetadata(properties, schemaName, newVersionNumber,
                            tableMap, client);
                case VERSION_LATEST_OR_NONE:
                default:
                    // Return specific version or null.
                    if (schema != null) {
                        LOGGER.info(String.format("Retrieved schema %s version %d in %d ms.",
                                schema.getSchemaName(), schema.getSchemaVersion(),
                                Instant.now().toEpochMilli() - beginRetrieval.toEpochMilli()));
                    } else {
                        LOGGER.info("Could not find schema {} in database {}.", schemaName,
                                properties.getDatabase());
                    }
                    return schema;
            }
        } finally {
            closeSchemaReader(schemaReader);
        }
    }

    private static LinkedHashMap<String, DocumentDbSchemaTable> buildTableMapById(
            final Map<String, DocumentDbSchemaTable> tableMap) {
        return tableMap.values().stream()
                .collect(Collectors.toMap(
                        DocumentDbSchemaTable::getId,
                        t -> t,
                        (o, d) -> o,
                        LinkedHashMap::new));
    }

    /**
     * Gets the table schema associated with the given table ID.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema.
     * @param schemaVersion the version of the schema.
     * @param tableId the table ID of the table.
     * @param client the {@link MongoClient} client.
     *
     * @return a {@link DocumentDbSchemaTable} that matches the table if it exists,
     * {@code null} if the table ID does not exist.
     */
    @NonNull
    @SneakyThrows
    public static DocumentDbSchemaTable getTable(
            final @NonNull DocumentDbConnectionProperties properties,
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull String tableId,
            final MongoClient client) {
        // Should only be in this map if we failed to write it.
        if (TABLE_MAP.containsKey(tableId)) {
            return TABLE_MAP.get(tableId);
        }
        // Otherwise, assume it's in the stored location.
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties, client);
        try {
            return schemaReader.readTable(schemaName, schemaVersion, tableId);
        } finally {
            closeSchemaReader(schemaReader);
        }
    }

    /**
     * Gets a map of table schema from the given set of table IDs.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the database schema.
     * @param schemaVersion the version of the database schema.
     * @param remainingTableIds the set of tables IDs.
     * @param client the {@link MongoClient} client.
     *
     * @return a map of table schema using the table ID as key.
     */
    @SneakyThrows
    public static Map<String, DocumentDbSchemaTable> getTables(
            final @NonNull DocumentDbConnectionProperties properties,
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull Set<String> remainingTableIds,
            final MongoClient client) {

        // Should only be in this map if we failed to write it.
        final LinkedHashMap<String, DocumentDbSchemaTable> map = remainingTableIds.stream()
                .filter(TABLE_MAP::containsKey)
                .collect(Collectors.toMap(
                        tableId -> tableId,
                        TABLE_MAP::get,
                        (o, d) -> d,
                        LinkedHashMap::new));
        if (map.size() == remainingTableIds.size()) {
            return map;
        }

        // Otherwise, assume it's in the stored location.
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties, client);
        try {
            return schemaReader.readTables(schemaName, schemaVersion, remainingTableIds)
                    .stream()
                    .collect(Collectors.toMap(
                            DocumentDbSchemaTable::getId,
                            table -> table,
                            (o, d) -> d,
                            LinkedHashMap::new));
        } finally {
            closeSchemaReader(schemaReader);
        }
    }

    /**
     * Removes all versions of the schema with given schema name.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema to remove.
     * @param client the {@link MongoClient} client.
     *
     * @throws SQLException if connection properties are incorrect.
     */
    public static void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final MongoClient client) throws SQLException {
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties, client);
        try {
            schemaWriter.remove(schemaName);
        } finally {
            closeSchemaWriter(schemaWriter);
        }
    }

    /**
     * Removes the specific version of the schema with given schema name and schema version.
     *
     * @param properties the connection properties.
     * @param schemaName the schema name.
     * @param schemaVersion the schema version.
     * @param client the {@link MongoClient} client.
     *
     * @throws SQLException if connection properties are incorrect.
     */
    public static void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final MongoClient client) throws SQLException {
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties, client);
        try  {
            schemaWriter.remove(schemaName, schemaVersion);
        } finally {
            closeSchemaWriter(schemaWriter);
        }
    }

    /**
     * Gets the list of all persisted schema.
     *
     * @param properties the connection properties.
     * @return a list of {@link DocumentDbSchema} schemas.
     * @throws SQLException if unable to connect.
     */
    public static List<DocumentDbSchema> getSchemaList(
            final DocumentDbConnectionProperties properties,
            final MongoClient client) throws SQLException {
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties, client);
        try {
            return schemaReader.list();
        } finally {
            closeSchemaReader(schemaReader);
        }
    }

    /**
     * Updates schema with the given table schema.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema.
     * @param schemaTables the collection of updated table schema.
     * @param client the {@link MongoClient} client.
     *
     * @throws SQLException if unable to connect or other exception.
     * @throws DocumentDbSchemaSecurityException if unable to write to the database due to
     * unauthorized user.
     */
    public static void update(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final Collection<DocumentDbSchemaTable> schemaTables,
            final MongoClient client) throws SQLException, DocumentDbSchemaSecurityException {
        DocumentDbSchema schema = get(properties, schemaName, VERSION_LATEST_OR_NONE, client);
        if (schema == null) {
            // This is intentional because the update will increment the version.
            final int schemaVersion = 0;
            schema = new DocumentDbSchema(
                    schemaName,
                    properties.getDatabase(),
                    schemaVersion,
                    new LinkedHashMap<>());
            LOGGER.info("A new schema {} will be created.", schemaName);
        }
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties, client);
        try {
            schemaWriter.update(schema, schemaTables);
        } finally {
            closeSchemaWriter(schemaWriter);
        }
    }

    private static DocumentDbSchema getNewDatabaseMetadata(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final MongoClient client) throws SQLException {
        LOGGER.debug("Beginning generation of new metadata.");
        final Instant beginGeneration = Instant.now();
        final DocumentDbSchema schema = getCollectionMetadataDirect(
                schemaName,
                schemaVersion,
                properties.getDatabase(),
                properties,
                tableMap,
                client);
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties, client);
        try {
            schemaWriter.write(schema, tableMap.values());
        } catch (DocumentDbSchemaSecurityException e) {
            TABLE_MAP.putAll(buildTableMapById(tableMap));
            LOGGER.warn(e.getMessage(), e);
        } finally {
            closeSchemaWriter(schemaWriter);
        }
        LOGGER.info(String.format("Successfully generated metadata in %d ms.",
                Instant.now().toEpochMilli() - beginGeneration.toEpochMilli()));
        return schema;
    }

    /**
     * Gets the metadata for all the collections in a DocumentDB database.
     *
     * @param properties the connection properties.
     *
     * @return a map of the collection metadata.
     */
    private static DocumentDbSchema getCollectionMetadataDirect(
            final String schemaName,
            final int schemaVersion,
            final String databaseName,
            final DocumentDbConnectionProperties properties,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final MongoClient client) throws SQLException {

        final MongoClientSettings settings = properties.buildMongoClientSettings();
        final MongoClient mongoClient = client != null
                ? client
                : MongoClients.create(settings);
        try {
            final MongoDatabase database = mongoClient.getDatabase(databaseName);
            for (String collectionName : getFilteredCollectionNames(database)) {
                final MongoCollection<BsonDocument> collection = database
                        .getCollection(collectionName, BsonDocument.class);
                final Iterator<BsonDocument> cursor = DocumentDbMetadataScanner
                        .getIterator(properties, collection);

                // Create the schema metadata.
                final Map<String, DocumentDbSchemaTable> tableSchemaMap =
                        DocumentDbTableSchemaGenerator.generate(
                                collectionName, cursor);
                tableMap.putAll(tableSchemaMap);
            }

            final Set<String> tableReferences = tableMap.values().stream()
                    .map(DocumentDbSchemaTable::getId)
                    .collect(Collectors.toSet());
            return new DocumentDbSchema(schemaName, schemaVersion, databaseName,
                    new Date(Instant.now().toEpochMilli()), tableReferences);
        } finally {
            if (client == null) {
                mongoClient.close();
            }
        }
    }

    private static List<String> getFilteredCollectionNames(final MongoDatabase database) {
        final Iterable<String> collectionNames = database.listCollectionNames();
        return StreamSupport
                .stream(collectionNames.spliterator(), false)
                .filter(c ->
                        !c.equals(DocumentDbSchemaReader.SCHEMA_COLLECTION)
                        && !c.equals(DocumentDbSchemaReader.TABLE_SCHEMA_COLLECTION))
                .collect(Collectors.toList());
    }

    private static void closeSchemaReader(final SchemaReader schemaReader) throws SQLException {
        try {
            schemaReader.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static void closeSchemaWriter(final SchemaWriter schemaWriter) throws SQLException {
        try {
            schemaWriter.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }
}
