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

package software.amazon.documentdb.jdbc.persist;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.NonNull;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_NAME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_VERSION_PROPERTY;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter.getDatabase;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter.getSchemaFilter;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter.getTableSchemaFilter;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter.isAuthorizationFailure;

/**
 * Implementation of the {@link SchemaReader} for DocumentDB storage.
 */
public class DocumentDbSchemaReader implements SchemaReader, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSchemaReader.class);
    private final DocumentDbConnectionProperties properties;
    private final MongoClient client;
    private final boolean closeClient;

    public static final String DEFAULT_SCHEMA_NAME = DocumentDbSchema.DEFAULT_SCHEMA_NAME;
    public static final String SCHEMA_COLLECTION = "_sqlSchemas";
    public static final String TABLE_SCHEMA_COLLECTION = "_sqlTableSchemas";
    static final CodecRegistry POJO_CODEC_REGISTRY = fromRegistries(
            getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder()
                    .register(DocumentDbSchema.class,
                            DocumentDbSchemaTable.class,
                            DocumentDbSchemaColumn.class,
                            DocumentDbMetadataColumn.class)
                    .build()));

    /**
     * Constructs a new {@link DocumentDbSchemaReader} with given connection properties.
     *
     * @param properties the connection properties for connecting to database.
     * @param client the {@link MongoClient} client.
     *
     */
    public DocumentDbSchemaReader(final @NonNull DocumentDbConnectionProperties properties,
            final MongoClient client) {
        this.properties = properties;
        this.client = client != null
                ? client
                : MongoClients.create(properties.buildMongoClientSettings());
        this.closeClient = client == null;
    }

    @Override
    public DocumentDbSchema read() {
        return read(DEFAULT_SCHEMA_NAME);
    }

    @Override
    public List<DocumentDbSchema> list() throws SQLException {
        final MongoDatabase database = client.getDatabase(properties.getDatabase());
        return getAllSchema(database);
    }

    @Nullable
    @Override
    public DocumentDbSchema read(final @NonNull String schemaName) {
        return read(schemaName, 0);
    }

    @Nullable
    @Override
    public DocumentDbSchema read(final @NonNull String schemaName, final int schemaVersion) {
        final MongoDatabase database = getDatabase(client, properties.getDatabase());
        return getSchema(schemaName, schemaVersion, database);
    }

    static DocumentDbSchema getSchema(
            final String schemaName,
            final int schemaVersion,
            final MongoDatabase database) {
        final MongoCollection<DocumentDbSchema> schemasCollection = database
                .getCollection(SCHEMA_COLLECTION, DocumentDbSchema.class);
        try {
            return schemasCollection
                    .find(getSchemaFilter(schemaName, schemaVersion))
                    .sort(descending(SCHEMA_VERSION_PROPERTY))
                    .first();
        } catch (MongoException e) {
            if (isAuthorizationFailure(e)) {
                LOGGER.warn(e.getMessage(), e);
                return null;
            }
            throw e;
        }
    }

    @Override
    public DocumentDbSchemaTable readTable(
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull String tableId) {
        final MongoDatabase database = getDatabase(client, properties.getDatabase());
        // Attempt to retrieve the table associated with the table ID.
        final MongoCollection<DocumentDbSchemaTable> tableSchemasCollection = database
                .getCollection(TABLE_SCHEMA_COLLECTION, DocumentDbSchemaTable.class);
        return tableSchemasCollection
                .find(getTableSchemaFilter(tableId))
                .first();
    }

    @Override
    public Collection<DocumentDbSchemaTable> readTables(
            final String schemaName,
            final int schemaVersion,
            final Set<String> tableIds) {
        final MongoDatabase database = getDatabase(client, properties.getDatabase());

        // Attempt to retrieve the tables associated with the table ID.
        final MongoCollection<DocumentDbSchemaTable> tableSchemasCollection = database
                .getCollection(TABLE_SCHEMA_COLLECTION, DocumentDbSchemaTable.class)
                .withCodecRegistry(POJO_CODEC_REGISTRY);
        final List<Bson> tableFilters = tableIds.stream()
                .map(DocumentDbSchemaWriter::getTableSchemaFilter)
                .collect(Collectors.toList());
        return StreamSupport.stream(
                        tableSchemasCollection.find(or(tableFilters)).spliterator(), false)
                .collect(Collectors.toList());
    }

    static List<DocumentDbSchema> getAllSchema(final MongoDatabase database) {
        final MongoCollection<DocumentDbSchema> schemasCollection = database
                .getCollection(SCHEMA_COLLECTION, DocumentDbSchema.class)
                .withCodecRegistry(POJO_CODEC_REGISTRY);
        try {
            final List<DocumentDbSchema> schemas = new ArrayList<>();
            schemasCollection
                    .find()
                    .sort(orderBy(ascending(SCHEMA_NAME_PROPERTY), ascending(SCHEMA_VERSION_PROPERTY)))
                    .forEach(schemas::add);
            return schemas;
        } catch (MongoException e) {
            if (isAuthorizationFailure(e)) {
                LOGGER.warn(e.getMessage(), e);
                return new ArrayList<>();
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (closeClient && client != null) {
            client.close();
        }
    }
}
