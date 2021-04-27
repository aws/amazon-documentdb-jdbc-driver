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

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.StoreEntryKey;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A service for retrieving DocumentDB database metadata.
 */
public class DocumentDbMetadataService {
    public static final int VERSION_LATEST = 0;
    public static final int VERSION_NEW = -1;

    private static final Map<StoreEntryKey, DocumentDbDatabaseSchemaMetadata> DOCUMENT_DB_DATABASE_METADATA_STORE =
            new ConcurrentHashMap<>();

    /**
     * Gets the latest or a new {@link DocumentDbDatabaseSchemaMetadata} instance based on the clientId
     * and properties. It uses a value of {@link DocumentDbMetadataService#VERSION_LATEST} for the
     * version to indicate to get the latest or create a new instance if none exists.
     *
     * @param clientId the client ID.
     * @param properties the connection properties.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbDatabaseSchemaMetadata get(final String clientId,
            final DocumentDbConnectionProperties properties) throws SQLException {
        return get(clientId, properties, VERSION_LATEST);
    }


    /**
     * Gets an existing {@link DocumentDbDatabaseSchemaMetadata} instance based on the clientId and version.
     *
     * @param clientId the client ID.
     * @param properties the connection properties.
     * @param version the version of the metadata. A version number of
     *                {@link DocumentDbMetadataService#VERSION_LATEST} indicates to get the latest
     *                or create a new instance.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance if the clientId and version exist, null,
     * otherwise.
     */
    public static DocumentDbDatabaseSchemaMetadata get(final String clientId,
            final DocumentDbConnectionProperties properties,
            final int version) throws SQLException {
        if (version == VERSION_LATEST) {
            final StoreEntryKey latestKey = DocumentDbDatabaseSchemaMetadata.findLatestKey(
                    DOCUMENT_DB_DATABASE_METADATA_STORE,
                    clientId, properties.getDatabase());

            if (latestKey != null) {
                return DOCUMENT_DB_DATABASE_METADATA_STORE.get(latestKey);
            }

            // A previous version doesn't exist, create one.
            final int newVersion = 1;
            return getNewDatabaseMetadata(clientId, properties, newVersion);
        } else if (version == VERSION_NEW) {
            final StoreEntryKey latestKey = DocumentDbDatabaseSchemaMetadata.findLatestKey(
                    DOCUMENT_DB_DATABASE_METADATA_STORE,
                    clientId, properties.getDatabase());
            final int newVersion = latestKey != null ? latestKey.getVersion() + 1 : 1;
            return getNewDatabaseMetadata(clientId, properties, newVersion);
        }

        // Find an existing one by key, if it exists.
        final StoreEntryKey key = StoreEntryKey.builder()
                .clientId(clientId)
                .databaseName(properties.getDatabase())
                .version(version)
                .build();
        return DOCUMENT_DB_DATABASE_METADATA_STORE.get(key);
    }

    private static DocumentDbDatabaseSchemaMetadata getNewDatabaseMetadata(final String clientId,
            final DocumentDbConnectionProperties properties, final int newVersion)
            throws SQLException {
        final StoreEntryKey newKey = StoreEntryKey.builder()
                .clientId(clientId)
                .databaseName(properties.getDatabase())
                .version(newVersion)
                .build();
        final DocumentDbDatabaseSchemaMetadata databaseMetadata = new DocumentDbDatabaseSchemaMetadata(
                newKey.getClientId(),
                newKey.getVersion(),
                getCollectionMetadataMapDirect(properties));
        DOCUMENT_DB_DATABASE_METADATA_STORE.put(newKey, databaseMetadata);
        return databaseMetadata;
    }

    /**
     * Gets the metadata for all the collections in a DocumentDB database.
     *
     * @param properties the connection properties.
     *
     * @return a map of the collection metadata.
     */
    private static ImmutableMap<String, DocumentDbSchemaCollection> getCollectionMetadataMapDirect(
            final DocumentDbConnectionProperties properties) throws SQLException {

        final ImmutableMap.Builder<String, DocumentDbSchemaCollection> builder =
                ImmutableMap.builder();
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        try (MongoClient client = MongoClients.create(settings)) {
            final MongoDatabase database = client.getDatabase(properties.getDatabase());

            for (String collectionName : database.listCollectionNames()) {
                final MongoCollection<BsonDocument> collection = database
                        .getCollection(collectionName, BsonDocument.class);
                final Iterator<BsonDocument> cursor = DocumentDbMetadataScanner
                        .getIterator(properties, collection);

                // Create the schema metadata.
                final DocumentDbSchemaCollection collectionMetadata =
                        DocumentDbCollectionMetadata.create(properties.getDatabase(), collectionName, cursor);
                builder.put(collectionName, collectionMetadata);
            }
        }

        return builder.build();
    }

}
