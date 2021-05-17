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
     * @throws DocumentDbSchemaException if the schema cannot be retrieved for some reason.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final DocumentDbConnectionProperties properties,
            final boolean refreshAll)
            throws SQLException, DocumentDbSchemaException {
        return get(DocumentDbSchema.DEFAULT_SCHEMA_NAME, properties, refreshAll);
    }


    /**
     * Gets the latest or a new {@link DocumentDbDatabaseSchemaMetadata} instance based on the
     * schemaName and properties. It uses a value of {@link DocumentDbMetadataService#VERSION_LATEST}
     * for the version to indicate to get the latest or create a new instance if none exists.
     *
     * @param schemaName the schema name.
     * @param properties the connection properties.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbDatabaseSchemaMetadata get(final String schemaName,
            final DocumentDbConnectionProperties properties)
            throws SQLException, DocumentDbSchemaException {
        return get(schemaName, properties, false);
    }

    /**
     * Gets the {@link DocumentDbDatabaseSchemaMetadata} by given schema name, connection properties
     * and an indicator of whether to refresh the metadata from the cached version.
     *
     * @param schemaName the schema name.
     * @param properties the connection properties.
     * @param refreshAll an indicator of whether to get refreshed metadata and ignore the cached
     *                   version.
     * @return a {@link DocumentDbDatabaseSchemaMetadata} instance.
     */
    public static DocumentDbDatabaseSchemaMetadata get(
            final String schemaName,
            final DocumentDbConnectionProperties properties,
            final boolean refreshAll) throws SQLException, DocumentDbSchemaException {

        final DocumentDbDatabaseSchemaMetadata metadata;
        final DocumentDbSchema schema = DocumentDbMetadataService
                .get(properties, schemaName,
                        refreshAll
                                ? DocumentDbMetadataService.VERSION_NEW
                                : DocumentDbMetadataService.VERSION_LATEST);
        if (schema != null) {
            schema.setGetTableFunction(tableId -> DocumentDbMetadataService
                    .getTable(properties, schemaName, schema.getSchemaVersion(), tableId));
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
            final int schemaVersion) throws SQLException, DocumentDbSchemaException {

        // Try to get it from the service.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata;
        final DocumentDbSchema schema = DocumentDbMetadataService
                .get(properties, schemaName, schemaVersion);
        if (schema != null) {
            // Setup lazy load based on table ID.
            schema.setGetTableFunction(tableId -> DocumentDbMetadataService
                    .getTable(properties, schemaName, schemaVersion, tableId));
            databaseMetadata = new DocumentDbDatabaseSchemaMetadata(schema);
        } else {
            databaseMetadata = null;
        }
        return databaseMetadata;
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
