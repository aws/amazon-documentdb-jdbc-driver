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

import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contains the metadata for a DocumentDB database including all of the collection and any
 * virtual tables.
 */
public final class DocumentDbDatabaseSchemaMetadata {
    private final DocumentDbSchema schema;

    /**
     * Gets the schema name for this database metadata.
     *
     * @return a String representing the schema name.
     */
    public String getSchemaName() {
        return schema.getSchemaName();
    }

    /**
     * Gets the version of this database metadata.
     *
     * @return a number representing the version of the database metadata.
     */
    public int getSchemaVersion() {
        return schema.getSchemaVersion();
    }

    public Map<String, DocumentDbSchemaTable> getTableSchemaMap() {
        return schema.getTableMap();
    }

    /**
     * Constructs a {@link DocumentDbDatabaseSchemaMetadata} instance from properties.
     *
     * @param schema the database schema.
     */
    protected DocumentDbDatabaseSchemaMetadata(
            final DocumentDbSchema schema) {
        this.schema = schema;
    }

    /**
     * Constructs a {@link DocumentDbDatabaseSchemaMetadata} instance using the default schema name
     * with an option to refresh all the table schema.
     *
     * @param properties the connection properties.
     * @param refreshAll an indicator of whether to refresh all the table schema.
     * @return a new {@link DocumentDbDatabaseSchemaMetadata} instance.
     *
     * @throws SQLException if a SQL exception occurs.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final DocumentDbConnectionProperties properties,
            final boolean refreshAll) throws SQLException {
        return get(properties, DocumentDbSchema.DEFAULT_SCHEMA_NAME, refreshAll);
    }


    /**
     * Gets the latest or a new {@link DocumentDbDatabaseSchemaMetadata} instance based on the
     * schemaName and properties. It uses a value of {@link DocumentDbMetadataService#VERSION_LATEST}
     * for the version to indicate to get the latest or create a new instance if none exists.
     *
     * @param properties the connection properties.
     * @param schemaName the schema name.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final DocumentDbConnectionProperties properties, final String schemaName)
            throws SQLException {
        return get(properties, schemaName, false);
    }

    /**
     * Gets the {@link DocumentDbDatabaseSchemaMetadata} by given schema name, connection properties
     * and an indicator of whether to refresh the metadata from the cached version.
     *
     * @param properties the connection properties.
     * @param schemaName the schema name.
     * @param refreshAll an indicator of whether to get refreshed metadata and ignore the cached
     *                   version.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final boolean refreshAll) throws SQLException {

        final DocumentDbDatabaseSchemaMetadata metadata;
        final int schemaVersion = refreshAll
                ? DocumentDbMetadataService.VERSION_NEW
                : DocumentDbMetadataService.VERSION_LATEST;
        final DocumentDbSchema schema = DocumentDbMetadataService
                .get(properties, schemaName, schemaVersion);
        if (schema != null) {
            // Setup lazy load based on table ID.
            setSchemaGetTableFunction(properties, schemaName, schemaVersion, schema);
            metadata = new DocumentDbDatabaseSchemaMetadata(schema);
        } else {
            metadata = null;
        }
        return metadata;
    }

    /**
     * Gets an existing {@link DocumentDbDatabaseSchemaMetadata} instance based on the schema name
     * and version.
     *
     * @param properties the properties of the connection.
     * @param schemaName the name of the schema.
     * @param schemaVersion the version of the schema. A version number of
     *                {@link DocumentDbMetadataService#VERSION_LATEST} indicates to get the latest
     *                or create a new instance.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance if the schema and version exist,
     * null otherwise.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final DocumentDbConnectionProperties properties, final String schemaName,
            final int schemaVersion) throws SQLException {

        // Try to get it from the service.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata;
        final DocumentDbSchema schema = DocumentDbMetadataService
                .get(properties, schemaName, schemaVersion);
        if (schema != null) {
            // Setup lazy load based on table ID.
            setSchemaGetTableFunction(properties, schemaName, schemaVersion, schema);
            databaseMetadata = new DocumentDbDatabaseSchemaMetadata(schema);
        } else {
            databaseMetadata = null;
        }
        return databaseMetadata;
    }

    /**
     * Removes all versions of the schema for the given schema name.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema.
     *
     * @throws SQLException if invalid connection properties.
     */
    public static void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName) throws SQLException {
        DocumentDbMetadataService.remove(properties, schemaName);
    }

    /**
     * Removes the specific schema for the given schema name and version.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema.
     * @param schemaVersion the version of the schema.
     *
     * @throws SQLException if invalid connection properties.
     */
    public static void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion) throws SQLException {
        DocumentDbMetadataService.remove(properties, schemaName, schemaVersion);
    }

    /**
     * Gets the list of all persisted schema.
     *
     * @param properties the connection properties.
     * @return a list of {@link DocumentDbSchema} schema.
     * @throws SQLException if unable to connect.
     */
    public static List<DocumentDbSchema> getSchemaList(
            final DocumentDbConnectionProperties properties) throws SQLException {
        final List<DocumentDbSchema> schemas = DocumentDbMetadataService.getSchemaList(properties);
        schemas.forEach(schema -> setSchemaGetTableFunction(
                properties, schema.getSchemaName(), schema.getSchemaVersion(), schema));
        return schemas;
    }

    private static void setSchemaGetTableFunction(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final DocumentDbSchema schema) {
        schema.setGetTableFunction(
                tableId -> DocumentDbMetadataService
                        .getTable(properties, schemaName, schemaVersion, tableId),
                remainingTableIds -> DocumentDbMetadataService
                        .getTables(properties, schemaName, schemaVersion, remainingTableIds));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentDbDatabaseSchemaMetadata)) {
            return false;
        }
        final DocumentDbDatabaseSchemaMetadata metadata = (DocumentDbDatabaseSchemaMetadata) o;
        return schema.equals(metadata.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }
}
