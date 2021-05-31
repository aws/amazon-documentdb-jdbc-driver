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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbSchemaStoreType;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.sql.SQLException;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperty.SCHEMA_PERSISTENCE_STORE;

/**
 * Factory class for creating schema readers and writers.
 */
public final class SchemaStoreFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaStoreFactory.class);

    /**
     * Disable instances of this class.
     */
    private SchemaStoreFactory() {
        throw new IllegalStateException("Factory class.");
    }

    /**
     * Creates a schema reader.
     *
     * @param properties the properties to examine for which type of schema reader store and the database
     *                   the schema will be reading from.
     * @return a {@link SchemaReader} instance.
     * @throws SQLException if unsupported schema store is specified.
     */
    public static SchemaReader createReader(final DocumentDbConnectionProperties properties)
            throws SQLException {
        final DocumentDbSchemaStoreType storeType = properties.getPersistedSchemaStore();
        switch (storeType) {
            case FILE:
                return new FileSchemaReader(properties.getDatabase());
            case DATABASE:
                return new DocumentDbSchemaReader(properties);
            default:
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.UNSUPPORTED_PROPERTY,
                        SCHEMA_PERSISTENCE_STORE.getName() + "=" + storeType.getName());
        }
    }

    /**
     * Creates a schema writer.
     *
     * @param properties the properties to examine for which type of schema writer store and the database
     *                   the schema will be writing to.
     * @return a {@link SchemaWriter} instance.
     * @throws SQLException if unsupported schema store is specified.
     */
    public static SchemaWriter createWriter(final DocumentDbConnectionProperties properties)
            throws SQLException {
        final DocumentDbSchemaStoreType storeType = properties.getPersistedSchemaStore();
        switch (storeType) {
            case FILE:
                return new FileSchemaWriter(properties.getDatabase());
            case DATABASE:
                return new DocumentDbSchemaWriter(properties);
            default:
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.UNSUPPORTED_PROPERTY,
                        SCHEMA_PERSISTENCE_STORE.getName() + "=" + storeType.getName());
        }
    }
}
