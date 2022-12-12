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

import com.mongodb.client.MongoClient;
import lombok.NonNull;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaSecurityException;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DocumentDbMetadataService {
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
    DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final MongoClient client) throws SQLException;

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
    DocumentDbSchema get(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final MongoClient client) throws SQLException;

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
    DocumentDbSchemaTable getTable(
            final @NonNull DocumentDbConnectionProperties properties,
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull String tableId,
            final MongoClient client);

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
    Map<String, DocumentDbSchemaTable> getTables(
            final @NonNull DocumentDbConnectionProperties properties,
            final @NonNull String schemaName,
            final int schemaVersion,
            final @NonNull Set<String> remainingTableIds,
            final MongoClient client);

    /**
     * Removes all versions of the schema with given schema name.
     *
     * @param properties the connection properties.
     * @param schemaName the name of the schema to remove.
     * @param client the {@link MongoClient} client.
     *
     * @throws SQLException if connection properties are incorrect.
     */
    void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final MongoClient client) throws SQLException;

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
    void remove(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final int schemaVersion,
            final MongoClient client) throws SQLException;

    /**
     * Gets the list of all persisted schema.
     *
     * @param properties the connection properties.
     * @return a list of {@link DocumentDbSchema} schemas.
     * @throws SQLException if unable to connect.
     */
    List<DocumentDbSchema> getSchemaList(
            final DocumentDbConnectionProperties properties,
            final MongoClient client) throws SQLException;

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
    void update(
            final DocumentDbConnectionProperties properties,
            final String schemaName,
            final Collection<DocumentDbSchemaTable> schemaTables,
            final MongoClient client) throws SQLException, DocumentDbSchemaSecurityException;
}
