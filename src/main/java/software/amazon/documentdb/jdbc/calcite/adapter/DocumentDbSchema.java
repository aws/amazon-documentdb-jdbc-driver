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

package software.amazon.documentdb.jdbc.calcite.adapter;

import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.utilities.LazyLinkedHashMap;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Provides a schema for DocumentDB
 */
public class DocumentDbSchema extends AbstractSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSchema.class);
    private Map<String, Table> tables;
    private final DocumentDbDatabaseSchemaMetadata databaseMetadata;
    private final String databaseName;

    /**
     * Constructs a new {@link DocumentDbSchema} from {@link DocumentDbDatabaseSchemaMetadata}.
     *
     * @param databaseMetadata the database metadata.
     */
    protected DocumentDbSchema(final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final DocumentDbConnectionProperties connectionProperties) {
        this.databaseMetadata = databaseMetadata;
        this.databaseName = connectionProperties.getDatabase();
        tables = null;
    }

    @SneakyThrows
    @Override
    protected Map<String, Table> getTableMap() {
        if (tables == null) {
            tables = new LazyLinkedHashMap<>(
                    new LinkedHashSet<>(databaseMetadata.getTableSchemaMap().keySet()),
                    this::getDocumentDbTable);
        }
        return tables;
    }

    @SneakyThrows
    private Table getDocumentDbTable(final String tableName) {
        final DocumentDbSchemaTable schemaTable = databaseMetadata
                .getTableSchemaMap().get(tableName);
        if (schemaTable == null) {
            // This will occur if the table schema is deleted after retrieving the
            // database schema.
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INCONSISTENT_SCHEMA,
                    tableName);
        }
        return new DocumentDbTable(schemaTable.getCollectionName(), schemaTable);
    }


    /**
     * Gets the name of the database.
     *
     * @return the name of the database.
     */
    public String getDatabaseName() {
        return databaseName;
    }
}
