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

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterSettings;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.ID_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.MODIFY_DATE_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_NAME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_VERSION_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SQL_NAME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.TABLES_PROPERTY;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader.POJO_CODEC_REGISTRY;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader.SCHEMA_COLLECTION;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader.TABLE_SCHEMA_COLLECTION;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader.getSchema;

/**
 * Implements the {@link SchemaWriter} interface for DocumentDB storage.
 */
public class DocumentDbSchemaWriter implements SchemaWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSchemaWriter.class);
    static final int MONGO_AUTHORIZATION_FAILURE = 13;
    private static final int MONGO_ALREADY_EXISTS = 48;

    private final DocumentDbConnectionProperties properties;

    /**
     * Constructs a new {@link DocumentDbSchemaWriter} with connection properties.
     *
     * @param properties the connection properties.
     */
    public DocumentDbSchemaWriter(final @NonNull DocumentDbConnectionProperties properties) {
        this.properties = properties;
    }

    @Override
    public void write(
            final @NonNull DocumentDbSchema schema,
            final @NonNull Collection<DocumentDbSchemaTable> tablesSchema)
            throws SQLException, DocumentDbSchemaSecurityException {

        try (MongoClient client = MongoClients.create(properties.buildMongoClientSettings())) {
            final MongoDatabase database = getDatabase(client, properties.getDatabase());
            final MongoCollection<DocumentDbSchema> schemasCollection = database
                    .getCollection(SCHEMA_COLLECTION, DocumentDbSchema.class);
            final MongoCollection<Document> tableSchemasCollection = database
                    .getCollection(TABLE_SCHEMA_COLLECTION);
            final boolean supportsMultiDocTransactions = supportsMultiDocTransactions(
                    client, database);

            ensureSchemaCollections(database);
            runTransactedSession(
                    client,
                    supportsMultiDocTransactions,
                    session -> upsertSchemaHandleSecurityException(
                            session,
                            schemasCollection,
                            tableSchemasCollection,
                            schema,
                            tablesSchema));
        }
    }

    @Override
    public void update(
            final @NonNull DocumentDbSchema schema,
            final @NonNull Collection<DocumentDbSchemaTable> tableSchemas) {

        final String schemaName = schema.getSchemaName();

        try (MongoClient client = MongoClients.create(properties.buildMongoClientSettings())) {
            final MongoDatabase database = getDatabase(client, properties.getDatabase());

            // Get the latest schema from storage.
            final DocumentDbSchema latestSchema = getSchema(schemaName, 0, database);
            final int schemaVersion = getSchemaVersion(schema, latestSchema);
            final Set<String> tableReferences = getTableReferences(latestSchema);

            // Determine which table references to update/delete.
            final MongoCollection<Document> tableSchemasCollection = database
                    .getCollection(TABLE_SCHEMA_COLLECTION);
            final Map<String, String> tableMap = getExistingTableMap(
                    tableReferences, tableSchemasCollection);
            final List<String> tableReferencesToDelete = resolveTableReferenceUpdates(
                    tableSchemas, tableReferences, tableMap);

            final boolean supportsMultiDocTransactions = supportsMultiDocTransactions(
                    client, database);
            runTransactedSession(
                    client,
                    supportsMultiDocTransactions,
                    session -> deleteAndUpsertSchemaHandleSecurityException(
                            session,
                            tableSchemasCollection,
                            database,
                            schemaName,
                            schemaVersion,
                            schema,
                            tableSchemas,
                            tableReferences,
                            tableReferencesToDelete));
        }
    }

    @Override
    public void remove(final @NonNull String schemaName) {
        remove(schemaName, 0);
    }

    @SneakyThrows
    @Override
    public void remove(final @NonNull String schemaName, final int schemaVersion) {
        // NOTE: schemaVersion <= 0 indicates "any" version.
        try (MongoClient client = MongoClients.create(properties.buildMongoClientSettings())) {
            final MongoDatabase database = getDatabase(client, properties.getDatabase());
            final MongoCollection<DocumentDbSchema> schemasCollection = database
                    .getCollection(SCHEMA_COLLECTION, DocumentDbSchema.class);
            final MongoCollection<Document> tableSchemasCollection = database
                    .getCollection(TABLE_SCHEMA_COLLECTION);

            final boolean supportsMultiDocTransactions = supportsMultiDocTransactions(
                    client, database);
            runTransactedSession(
                    client,
                    supportsMultiDocTransactions,
                    session -> deleteSchema(
                            session,
                            schemasCollection,
                            tableSchemasCollection,
                            schemaName,
                            schemaVersion));
        }
    }

    private static void runTransactedSession(
            final MongoClient client,
            final boolean supportsMultiDocTransactions,
            final Consumer<ClientSession> process) {
        final ClientSession session = supportsMultiDocTransactions
                ? client.startSession()
                : null;
        try {
            maybeStartTransaction(session);
            process.accept(session);
            maybeCommitTransaction(session);
        } catch (Exception e) {
            maybeAbortTransaction(session);
            throw e;
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @SneakyThrows
    private void deleteAndUpsertSchemaHandleSecurityException(
            final ClientSession session,
            final MongoCollection<Document> tableSchemasCollection,
            final MongoDatabase database,
            final String schemaName,
            final int schemaVersion,
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tableSchemas,
            final Set<String> tableReferences,
            final List<String> tableReferencesToDelete) {
        final MongoCollection<DocumentDbSchema> schemaCollection = database
                .getCollection(SCHEMA_COLLECTION, DocumentDbSchema.class);
        try {
            deleteAndUpsertSchema(session,
                    schemaCollection,
                    tableSchemasCollection,
                    schemaName,
                    schemaVersion,
                    schema,
                    tableSchemas,
                    tableReferences,
                    tableReferencesToDelete);
        } catch (MongoException e) {
            if (isAuthorizationFailure(e)) {
                throw new DocumentDbSchemaSecurityException(e.getMessage(), e);
            }
            throw e;
        }
    }

    @SneakyThrows
    private void deleteSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemasCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final String schemaName,
            final int schemaVersion) {
        final Bson schemaFilter = getSchemaFilter(schemaName, schemaVersion);
        for (DocumentDbSchema schema : schemasCollection.find(schemaFilter)) {
            // Delete the table schemas associated with this database schema.
            deleteTableSchemas(session, tableSchemasCollection, schema.getTableReferences());
            // Delete the database schema.
            deleteDatabaseSchema(session, schemasCollection, schemaName, schema.getSchemaVersion());
        }
    }

    // Use @SneakyThrows to allow it to be used in a lambda expression.
    @SneakyThrows
    private void upsertSchemaHandleSecurityException(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemasCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tablesSchema) {
        try {
            upsertSchema(session, schemasCollection, tableSchemasCollection, schema, tablesSchema);
        } catch (MongoException e) {
            if (isAuthorizationFailure(e)) {
                throw new DocumentDbSchemaSecurityException(e.getMessage(), e);
            }
            throw e;
        }
    }

    private void upsertSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemasCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tablesSchema) throws SQLException {
        for (DocumentDbSchemaTable tableSchema : tablesSchema) {
            upsertTableSchema(session, tableSchemasCollection, tableSchema,
                    schema.getSchemaName());
        }
        upsertDatabaseSchema(session, schemasCollection, schema);
    }

    private void deleteAndUpsertSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemaCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final String schemaName,
            final int schemaVersion,
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tableSchemas,
            final Set<String> tableReferences,
            final List<String> tableReferencesToDelete) throws SQLException {
        deletePreviousSchema(session, schemaCollection, tableSchemasCollection,
                schemaName, tableReferencesToDelete);
        upsertNewSchema(session, schemaCollection, tableSchemasCollection, schemaName,
                schemaVersion + 1, schema, tableSchemas, tableReferences);
    }

    private void ensureSchemaCollections(final MongoDatabase database)
            throws DocumentDbSchemaSecurityException {
        createCollectionIfNotExists(database, SCHEMA_COLLECTION);
        createCollectionIfNotExists(database, TABLE_SCHEMA_COLLECTION);
    }

    private Map<String, String> getExistingTableMap(
            final Set<String> tableReferences,
            final MongoCollection<Document> tableSchemasCollection) {
        return Lists.newArrayList(() ->
                getTableSchemaSqlNames(tableSchemasCollection, tableReferences)).stream()
                .collect(Collectors.toMap(
                        t -> t.getString(SQL_NAME_PROPERTY), t -> t.getString(ID_PROPERTY)));
    }

    private void upsertNewSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemaCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final String schemaName,
            final int schemaVersion,
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tableSchemas,
            final Set<String> tableReferences) throws SQLException {
        // Insert/Update the table schema.
        for (DocumentDbSchemaTable tableSchema : tableSchemas) {
            upsertTableSchema(session, tableSchemasCollection, tableSchema, schemaName);
        }
        // Insert/Update the database schema
        final DocumentDbSchema newSchema = new DocumentDbSchema(
                schema.getSchemaName(),
                schemaVersion,
                schema.getSqlName(),
                new Date(Instant.now().toEpochMilli()),
                tableReferences);
        upsertDatabaseSchema(session, schemaCollection, newSchema);
    }

    private void deletePreviousSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemaCollection,
            final MongoCollection<Document> tableSchemasCollection,
            final String schemaName,
            final List<String> tableReferencesToDelete) throws SQLException {
        // Delete the table schema associated with this database schema.
        deleteTableSchemas(session, tableSchemasCollection, tableReferencesToDelete);
        // Delete existing schema
        deleteDatabaseSchema(session, schemaCollection, schemaName, 0);
    }

    private List<String> resolveTableReferenceUpdates(
            final Collection<DocumentDbSchemaTable> tableSchemas,
            final Set<String> tableReferences,
            final Map<String, String> tableMap) {
        final List<String> tableReferencesToDelete = new ArrayList<>();
        for (DocumentDbSchemaTable tableSchema : tableSchemas) {
            if (tableMap.containsKey(tableSchema.getSqlName())) {
                // remove existing
                final String tableId = tableMap.get(tableSchema.getSqlName());
                tableReferencesToDelete.add(tableId);
                // update schema table references list
                tableReferences.remove(tableId);
                tableReferences.add(tableSchema.getId());
                // write the table schema
            }
        }
        return tableReferencesToDelete;
    }

    private HashSet<String> getTableReferences(final DocumentDbSchema latestSchema) {
        return latestSchema != null
                ? new HashSet<>(latestSchema.getTableReferences())
                : new HashSet<>();
    }

    private int getSchemaVersion(
            final DocumentDbSchema schema,
            final DocumentDbSchema latestSchema) {
        return latestSchema != null
                ? Math.max(latestSchema.getSchemaVersion(), schema.getSchemaVersion())
                : schema.getSchemaVersion();
    }

    static MongoDatabase getDatabase(final MongoClient client, final String databaseName) {
        return client.getDatabase(databaseName)
                .withCodecRegistry(POJO_CODEC_REGISTRY);
    }

    private Iterator<Document> getTableSchemaSqlNames(
            final MongoCollection<Document> tableSchemasCollection,
            final Set<String> tableReferences) {
        if (!tableReferences.isEmpty()) {
            final List<Bson> idFilter = tableReferences.stream()
                    .map(tableId -> eq("_id", tableId))
                    .collect(Collectors.toList());
            return tableSchemasCollection
                    .find(or(idFilter))
                    .projection(Projections.include(ID_PROPERTY, SQL_NAME_PROPERTY))
                    .iterator();
        }
        return Collections.emptyIterator();
    }

    private static boolean supportsMultiDocTransactions(
            final MongoClient client,
            final MongoDatabase database) {
        final boolean supportsMultiDocTransactions;
        final ClusterSettings settings = client.getClusterDescription().getClusterSettings();
        final Document buildInfo = database.runCommand(Document.parse("{ \"buildInfo\": 1 }"));
        final List<Integer> version = buildInfo.getList("versionArray", Integer.class);
        supportsMultiDocTransactions =
                settings.getRequiredReplicaSetName() != null
                        && version != null && !version.isEmpty()
                        && version.get(0) >= 4;
        return supportsMultiDocTransactions;
    }

    private static void maybeAbortTransaction(
            final ClientSession session) {
        if (session != null) {
            session.abortTransaction();
        }
    }

    private static void maybeCommitTransaction(
            final ClientSession session) {
        if (session != null) {
            session.commitTransaction();
        }
    }

    private static void maybeStartTransaction(
            final ClientSession session) {
        if (session != null) {
            session.startTransaction();
        }
    }

    private static void upsertDatabaseSchema(
            final @Nullable ClientSession session,
            final @NonNull MongoCollection<DocumentDbSchema> schemasCollection,
            final @NonNull DocumentDbSchema schema) throws SQLException {
        final Bson schemaFilter = getSchemaFilter(schema.getSchemaName(), schema.getSchemaVersion());
        final Bson schemaUpdate = getSchemaUpdate(schema);
        final UpdateOptions upsertOption = new UpdateOptions().upsert(true);
        final UpdateResult result = session != null
                ? schemasCollection.updateOne(session, schemaFilter, schemaUpdate, upsertOption)
                : schemasCollection.updateOne(schemaFilter, schemaUpdate, upsertOption);
        if (!result.wasAcknowledged()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UPSERT_SCHEMA_FAILED,
                    schema.getSchemaName());
        }
    }

    private static void upsertTableSchema(
            final @Nullable ClientSession session,
            final @NonNull MongoCollection<Document> tableSchemasCollection,
            final @NonNull DocumentDbSchemaTable tableSchema,
            final @NonNull String schemaName) throws SQLException {
        final Bson tableSchemaFilter = getTableSchemaFilter(tableSchema.getId());
        final Bson tableSchemaUpdate = getTableSchemaUpdate(tableSchema);
        final UpdateOptions upsertOption = new UpdateOptions().upsert(true);
        final UpdateResult result = session != null
                ? tableSchemasCollection.updateOne(session,
                        tableSchemaFilter, tableSchemaUpdate, upsertOption)
                : tableSchemasCollection.updateOne(
                        tableSchemaFilter, tableSchemaUpdate, upsertOption);
        if (!result.wasAcknowledged()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UPSERT_SCHEMA_FAILED,
                    schemaName);
        }
    }

    private static void deleteDatabaseSchema(
            final ClientSession session,
            final MongoCollection<DocumentDbSchema> schemasCollection,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        final Bson schemaFilter = getSchemaFilter(schemaName, schemaVersion);
        final DeleteResult result = session != null
                ? schemasCollection.deleteOne(session, schemaFilter)
                : schemasCollection.deleteOne(schemaFilter);
        if (!result.wasAcknowledged() || result.getDeletedCount() < 1) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.DELETE_SCHEMA_FAILED, schemaName);
        }
    }

    private static void deleteTableSchemas(
            final ClientSession session,
            final MongoCollection<Document> tableSchemasCollection,
            final Collection<String> tableReferences) throws SQLException {
        final List<Bson> tableReferencesFilter = tableReferences.stream()
                .map(DocumentDbSchemaWriter::getTableSchemaFilter)
                .collect(Collectors.toList());
        if (!tableReferencesFilter.isEmpty()) {
            final Bson allTableReferencesFilter = or(tableReferencesFilter);
            final DeleteResult result = session != null
                    ? tableSchemasCollection.deleteMany(session, allTableReferencesFilter)
                    : tableSchemasCollection.deleteMany(allTableReferencesFilter);
            if (!result.wasAcknowledged() || result.getDeletedCount() != tableReferencesFilter
                    .size()) {
                throw SqlError.createSQLException(LOGGER,
                        SqlState.DATA_EXCEPTION,
                        SqlError.DELETE_TABLE_SCHEMA_FAILED);
            }
        }
    }

    private static Bson getTableSchemaUpdate(final DocumentDbSchemaTable schemaTable) {
        return combine(
                set("sqlName", schemaTable.getSqlName()),
                set("collectionName", schemaTable.getCollectionName()),
                set("modifyDate", schemaTable.getModifyDate()),
                set("columns", schemaTable.getColumnMap().values().stream()
                        .map(c -> new DocumentDbSchemaColumn(
                                c.getFieldPath(),
                                c.getSqlName(),
                                c.getSqlType(),
                                c.getDbType(),
                                c.isIndex(),
                                c.isPrimaryKey(),
                                c.getForeignKeyTableName(),
                                c.getForeignKeyColumnName()))
                        .collect(Collectors.toList())),
                setOnInsert("uuid", schemaTable.getUuid()));
    }

    static Bson getTableSchemaFilter(final String tableId) {
        return eq("_id", tableId);
    }

    private static Bson getSchemaUpdate(final DocumentDbSchema schema) {
        return combine(
                set(SQL_NAME_PROPERTY, schema.getSqlName()),
                set(MODIFY_DATE_PROPERTY, schema.getModifyDate()),
                set(TABLES_PROPERTY, schema.getTableReferences()),
                setOnInsert(SCHEMA_NAME_PROPERTY, schema.getSchemaName()),
                setOnInsert(SCHEMA_VERSION_PROPERTY, schema.getSchemaVersion()));
    }

    static Bson getSchemaFilter(final String schemaName, final int schemaVersion) {
        return schemaVersion > 0
                ? and(
                        eq(SCHEMA_NAME_PROPERTY, schemaName),
                        eq(SCHEMA_VERSION_PROPERTY, schemaVersion))
                : eq(SCHEMA_NAME_PROPERTY, schemaName);
    }

    private void createCollectionIfNotExists(
            final MongoDatabase database,
            final String collectionName) throws DocumentDbSchemaSecurityException {
        if (Streams.stream(database.listCollectionNames())
                .anyMatch(c -> c.equals(collectionName))) {
            return;
        }
        try {
            database.createCollection(collectionName);
        } catch (MongoException e) {
            // Handle race condition if it created after testing for existence.
            if (e.getCode() == MONGO_ALREADY_EXISTS) {
                LOGGER.info(String.format(
                        "Schema collection '%s' already exists.", collectionName));
            } else if (isAuthorizationFailure(e)) {
                throw new DocumentDbSchemaSecurityException(e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    static boolean isAuthorizationFailure(final MongoException e) {
        return e.getCode() == MONGO_AUTHORIZATION_FAILURE
                || "authorization failure".equalsIgnoreCase(e.getMessage());
    }
}
