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
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.persist.SchemaReader;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A service for retrieving DocumentDB database metadata.
 */
public class DocumentDbMetadataService {
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
        final SchemaReader schemaReader = getSchemaReader(properties);
        final DocumentDbSchema schema;
        if (schemaVersion == VERSION_LATEST) {
            // Get the latest version
            schema = schemaReader.read(schemaName);
            if (schema != null) {
                return schema;
            }
            // A previous version doesn't exist, create one.
            final int newVersion = 1;
            return getNewDatabaseMetadata(properties, schemaName, newVersion);
        } else if (schemaVersion == VERSION_NEW) {
            // Get a new version.
            schema = schemaReader.read(schemaName);
            final int newVersion = schema != null ? schema.getSchemaVersion() + 1 : 1;
            return getNewDatabaseMetadata(properties, schemaName, newVersion);
        }

        // Get a specific version - might not exist.
        return getSchemaMetadata(properties, schemaName, schemaVersion);
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

        final SchemaReader schemaReader = getSchemaReader(properties);
        return schemaReader.readTable(schemaName, schemaVersion, tableId);
    }

    private static DocumentDbSchema getSchemaMetadata(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        final SchemaReader schemaReader = getSchemaReader(properties);
        return schemaReader.read(schemaName, schemaVersion);
    }

    private static DocumentDbSchema getNewDatabaseMetadata(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        final Map<String, DocumentDbSchemaTable> tableMap = new LinkedHashMap<>();
        final DocumentDbSchema schema = getCollectionMetadataDirect(
                schemaName,
                schemaVersion,
                properties.getDatabase(),
                properties,
                tableMap);
        final SchemaWriter schemaWriter = getSchemaWriter(properties);
        schemaWriter.write(schema, tableMap.values());
        return schema;
    }

    private static SchemaWriter getSchemaWriter(final DocumentDbConnectionProperties properties)
            throws SQLException {
        return SchemaStoreFactory.createWriter(properties);
    }

    private static SchemaReader getSchemaReader(
            final DocumentDbConnectionProperties properties) throws SQLException {
        return SchemaStoreFactory.createReader(properties);
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

            for (String collectionName : database.listCollectionNames()) {
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
                    .map(t -> t.getId())
                    .collect(Collectors.toSet());
            return new DocumentDbSchema(schemaName, schemaVersion, databaseName,
                    new Date(Instant.now().toEpochMilli()), tableReferences);
        }
    }

}
