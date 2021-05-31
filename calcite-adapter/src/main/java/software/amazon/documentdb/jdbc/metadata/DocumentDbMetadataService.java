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
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A service for retrieving DocumentDB database metadata.
 */
public class DocumentDbMetadataService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMetadataService.class);
    private static final Map<String, DocumentDbSchemaTable> TABLE_MAP = new ConcurrentHashMap<>();

    public static final int VERSION_LATEST = 0;
    public static final int VERSION_NEW = -1;

    /**
     * Gets the latest or a new {@link DocumentDbDatabaseSchemaMetadata} instance based on the
     * schemaName and properties. It uses a value of {@link DocumentDbMetadataService#VERSION_LATEST}
     * for the version to indicate to get the latest or create a new instance if none exists.
     *
     * @param properties the connection properties.
     * @param schemaName the client ID.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName) throws SQLException {
        return get(properties, schemaName, VERSION_LATEST);
    }


    /**
     * Gets an existing {@link DocumentDbDatabaseSchemaMetadata} instance based on the clientId and
     * version.
     *
     * @param properties the connection properties.
     * @param schemaName the client ID.
     * @param schemaVersion the version of the metadata. A version number of
     *                {@link DocumentDbMetadataService#VERSION_LATEST} indicates to get the latest
     *                or create a new instance.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance if the clientId and version exist,
     * {@code null} otherwise.
     */
    @Nullable
    public static DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties);
        final Map<String, DocumentDbSchemaTable> tableMap = new LinkedHashMap<>();
        DocumentDbSchema schema;
        if (schemaVersion == VERSION_LATEST) {
            // Get the latest version
            schema = schemaReader.read(schemaName);
            if (schema != null) {
                return schema;
            }
            // A previous version doesn't exist, create one.
            final int newVersion = 1;
            schema = getNewDatabaseMetadata(properties, schemaName, newVersion, tableMap);
            return schema;
        } else if (schemaVersion == VERSION_NEW) {
            // Get a new version.
            schema = schemaReader.read(schemaName);
            final int newVersion = schema != null ? schema.getSchemaVersion() + 1 : 1;
            schema = getNewDatabaseMetadata(properties, schemaName, newVersion, tableMap);
            return schema;
        }

        // Get a specific version - might not exist.
        return getSchemaMetadata(properties, schemaName, schemaVersion);
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
            final @NonNull String tableId) {
        // Should only be in this map if we failed to write it.
        if (TABLE_MAP.containsKey(tableId)) {
            return TABLE_MAP.get(tableId);
        }
        // Otherwise, assume it's in the stored location.
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties);
        return schemaReader.readTable(schemaName, schemaVersion, tableId);
    }

    /**
     * Gets a map of table schema from the given set of table IDs.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the database schema.
     * @param schemaVersion the version of the database schema.
     * @param remainingTableIds the set of tables IDs.
     *
     * @return a map of table schema using the table ID as key.
     */
    @SneakyThrows
    public static Map<String, DocumentDbSchemaTable> getTables(
            final @NonNull DocumentDbConnectionProperties properties,
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull Set<String> remainingTableIds) {

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
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties);
        return schemaReader.readTables(schemaName, schemaVersion, remainingTableIds)
                .stream()
                .collect(Collectors.toMap(
                        DocumentDbSchemaTable::getId,
                        table -> table,
                        (o, d) -> d,
                        LinkedHashMap::new));
    }

    private static DocumentDbSchema getSchemaMetadata(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        final SchemaReader schemaReader = SchemaStoreFactory.createReader(properties);
        return schemaReader.read(schemaName, schemaVersion);
    }

    private static DocumentDbSchema getNewDatabaseMetadata(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final Map<String, DocumentDbSchemaTable> tableMap) throws SQLException {
        final DocumentDbSchema schema = getCollectionMetadataDirect(
                schemaName,
                schemaVersion,
                properties.getDatabase(),
                properties,
                tableMap);
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties);
        try {
            schemaWriter.write(schema, tableMap.values());
        } catch (DocumentDbSchemaSecurityException e) {
            TABLE_MAP.putAll(buildTableMapById(tableMap));
            LOGGER.warn(e.getMessage(), e);
        }
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
            final Map<String, DocumentDbSchemaTable> tableMap) throws SQLException {

        final MongoClientSettings settings = properties.buildMongoClientSettings();
        try (MongoClient client = MongoClients.create(settings)) {
            final MongoDatabase database = client.getDatabase(databaseName);
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

}
