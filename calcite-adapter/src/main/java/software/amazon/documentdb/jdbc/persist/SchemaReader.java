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

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface SchemaReader extends AutoCloseable {

    /**
     * Reads the latest version of the default schema for current database.
     *
     * @return a {@link DocumentDbSchema} schema for the database.
     */
    DocumentDbSchema read();

    /**
     * Reads the list of all schema in the current database.
     *
     * @return a list of {@link DocumentDbSchema} items for the current database.
     */
    List<DocumentDbSchema> list() throws SQLException;

    /**
     * Reads the latest version of the specified schema for current database.
     *
     * @param schemaName the name of the schema to read.
     * @return a {@link DocumentDbSchema} schema for the database, or {@code null}, if not found.
     */
    @Nullable DocumentDbSchema read(final String schemaName);

    /**
     * Reads the given version of the specified schema for current database.
     *
     * @param schemaName the name of the schema to read.
     * @param schemaVersion the specific version of the schema.
     * @return a {@link DocumentDbSchema} schema for the database, or {@code null}, if not found.
     */
    @Nullable DocumentDbSchema read(final String schemaName, final int schemaVersion);

    /**
     * Reads the table schema for the given table ID.
     *
     * @param schemaName the name of the schema to read.
     * @param schemaVersion the specific version of the schema.
     * @param tableId the table ID for the table schema.
     * @return a {@link DocumentDbSchemaTable} table schema.
     */
    DocumentDbSchemaTable readTable(final String schemaName, final int schemaVersion, final String tableId);

    /**
     * Reads the table schema for the given set of table ID.
     *
     * @param schemaName the name of the database schema.
     * @param schemaVersion the version of the database schema.
     * @param tableIds the set of table IDs to read.
     *
     * @return a collection of {@link DocumentDbSchemaTable} table schema.
     */
    Collection<DocumentDbSchemaTable> readTables(
            final String schemaName,
            final int schemaVersion,
            final Set<String> tableIds);
}
