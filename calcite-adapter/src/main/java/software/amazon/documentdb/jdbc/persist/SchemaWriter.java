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

import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.sql.SQLException;
import java.util.Collection;

/**
 * The SchemaWriter interface defines ways to write the schema to persistent storage.
 *
 * Writing the schema can be done in one of two ways. A complete update of database and table
 * schema - or just updating a single table.
 */
public interface SchemaWriter extends AutoCloseable {
    /**
     * Writes the complete schema for the database including any associated tables.
     *
     * @param schema the schema to write.
     */
    void write(
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tablesSchema)
            throws SQLException, DocumentDbSchemaSecurityException;

    /**
     * Writes only the specific table schemaName.
     *  @param schema the database schema.
     * @param tableSchemas the table schema to update.
     */
    void update(
            final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tableSchemas)
            throws SQLException, DocumentDbSchemaSecurityException;

    /**
     * Remove all versions of the schema associated with the given schema name.
     *
     * @param schemaName the name of the database schema.
     */
    void remove(final String schemaName);

    /**
     * Remove the specific version of the schema associated with the given schema name.
     *
     * @param schemaName the name of the database schema.
     * @param schemaVersion the version of the schema.
     */
    void remove(final String schemaName, final int schemaVersion);
}
