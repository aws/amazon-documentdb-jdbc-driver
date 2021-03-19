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
import lombok.Builder;
import lombok.Getter;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains the metadata for a DocumentDB database including all of the collection and any
 * virtual tables.
 */
public final class DocumentDbDatabaseMetadata {
    private static final Map<StoreEntryKey, DocumentDbDatabaseMetadata> DOCUMENT_DB_DATABASE_METADATA_STORE =
            new ConcurrentHashMap<>();

    private final ImmutableMap<String, DocumentDbCollectionMetadata> collectionMetadataMap;
    private final String clientId;
    private final int version;

    /**
     * Gets the client ID for this database metadata.
     *
     * @return a String representing the client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the version of this database metadata.
     *
     * @return a number representing the version of the database metadata.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Gets the map of collection metadata for the database.
     *
     * @return a map of {@link DocumentDbCollectionMetadata} keyed by collection name.
     */
    public ImmutableMap<String, DocumentDbCollectionMetadata> getCollectionMetadataMap() {
        return collectionMetadataMap;
    }

    /**
     * Constructs a {@link DocumentDbDatabaseMetadata} instance from properties.
     *
     * @param clientId the client ID for this database
     * @param version the version of the metadata.
     * @param collectionMetadataMap the map of {@link DocumentDbCollectionMetadata} keyed by
     *                              collection name for this database.
     */
    DocumentDbDatabaseMetadata(
            final String clientId,
            final int version,
            final ImmutableMap<String, DocumentDbCollectionMetadata> collectionMetadataMap) {
        this.clientId = clientId;
        this.version = version;
        this.collectionMetadataMap = collectionMetadataMap;
    }

    /**
     * Gets the latest or a new {@link DocumentDbDatabaseMetadata} instance based on the clientId
     * and properties. It uses a value of {@link DocumentDbMetadataService#VERSION_LATEST} for the
     * version to indicate to get the latest or create a new instance if none exists.
     *
     * @param clientId the client ID of the caller.
     * @param properties the connection properties.
     * @return a {@link DocumentDbDatabaseMetadata} instance.
     */
    public static DocumentDbDatabaseMetadata get(final String clientId,
            final DocumentDbConnectionProperties properties) throws SQLException {
        return get(clientId, properties, false);
    }

    /**
     * Gets the {@link DocumentDbDatabaseMetadata} by given client ID, connection properties
     * and an indicator of whether to refresh the metadata from the cached version.
     *
     * @param clientId the client ID.
     * @param properties the connection properties.
     * @param refreshAll an indicator of whether to get refreshed metadata and ignore the cached
     *                   version.
     * @return a {@link DocumentDbDatabaseMetadata} instance.
     */
    public static DocumentDbDatabaseMetadata get(final String clientId,
            final DocumentDbConnectionProperties properties,
            final boolean refreshAll) throws SQLException {

        if (refreshAll) {
            final DocumentDbDatabaseMetadata databaseMetadata = DocumentDbMetadataService
                    .get(clientId, properties, DocumentDbMetadataService.VERSION_NEW);
            if (databaseMetadata != null) {
                final StoreEntryKey key = StoreEntryKey.builder()
                        .clientId(clientId)
                        .databaseName(properties.getDatabase())
                        .version(databaseMetadata.version)
                        .build();
                DOCUMENT_DB_DATABASE_METADATA_STORE.put(key, databaseMetadata);
            }
            return databaseMetadata;
        }

        StoreEntryKey key = findLatestKey(
                DOCUMENT_DB_DATABASE_METADATA_STORE, clientId, properties.getDatabase());
        if (key != null) {
            return DOCUMENT_DB_DATABASE_METADATA_STORE.get(key);
        }

        final DocumentDbDatabaseMetadata databaseMetadata = DocumentDbMetadataService
                .get(clientId, properties);
        if (databaseMetadata != null) {
            key = StoreEntryKey.builder()
                    .clientId(clientId)
                    .databaseName(properties.getDatabase())
                    .version(databaseMetadata.version)
                    .build();
            DOCUMENT_DB_DATABASE_METADATA_STORE.put(key, databaseMetadata);
        }
        return databaseMetadata;
    }

    /**
     * Gets an existing {@link DocumentDbDatabaseMetadata} instance based on the clientId and version.
     * @param clientId the clientId of the metadata.
     * @param properties the properties of the connection.
     * @param version the version of the metadata. A version number of
     *                {@link DocumentDbMetadataService#VERSION_LATEST} indicates to get the latest
     *                or create a new instance.
     * @return a {@link DocumentDbDatabaseMetadata} instance if the clientId and version exist, null,
     * otherwise.
     */
    public static DocumentDbDatabaseMetadata get(final String clientId,
            final DocumentDbConnectionProperties properties,
            final int version) throws SQLException {
        final StoreEntryKey key = StoreEntryKey.builder()
                .clientId(clientId)
                .databaseName(properties.getDatabase())
                .version(version)
                .build();

        // Try to get it from the local cache.
        DocumentDbDatabaseMetadata databaseMetadata = DOCUMENT_DB_DATABASE_METADATA_STORE
                .get(key);
        if (databaseMetadata != null) {
            return databaseMetadata;
        }

        // Try to get it from the service.
        databaseMetadata = DocumentDbMetadataService.get(clientId, properties, version);
        if (databaseMetadata != null) {
            DOCUMENT_DB_DATABASE_METADATA_STORE.put(key, databaseMetadata);
        }
        return databaseMetadata;
    }

    /**
     * Finds the key for the latest version of the the database metadata, if it exists.
     *
     * @param store the store (map) to search.
     * @param clientId the client ID in the store.
     * @param databaseName the database name of the metadata.
     * @return a non-null {@link StoreEntryKey} entry if an entry exists, null, otherwise.
     */
    static StoreEntryKey findLatestKey(
            final Map<StoreEntryKey, DocumentDbDatabaseMetadata> store,
            final String clientId,
            final String databaseName) {
        return store.keySet().stream()
                .filter(entry -> entry.getClientId().equals(clientId)
                        && entry.getDatabaseName().equals(databaseName))
                .max(Comparator.comparing(StoreEntryKey::getVersion))
                .orElse(null);
    }

    /**
     * The key for storing database metadata.
     */
    @Getter
    @Builder
    static class StoreEntryKey {
        /** Gets the client ID for the entry key. */
        private final String clientId;
        /** Gets the name of the database for the entry key. */
        private final String databaseName;
        /** Gets the version of the entry key. */
        private final int version;

        @Override
        public boolean equals(final Object obj) {
            return obj == this
                    || obj instanceof StoreEntryKey
                    && clientId.equals(((StoreEntryKey) obj).clientId)
                    && databaseName.equals(((StoreEntryKey) obj).databaseName)
                    && version == ((StoreEntryKey) obj).version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, databaseName, version);
        }
    }
}
