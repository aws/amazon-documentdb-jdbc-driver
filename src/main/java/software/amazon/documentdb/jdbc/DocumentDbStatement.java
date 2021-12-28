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

package software.amazon.documentdb.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Statement;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.SQLException;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.FETCH_SIZE_DEFAULT;

/**
 * DocumentDb implementation of DatabaseMetadata.
 */
class DocumentDbStatement extends Statement implements java.sql.Statement {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbStatement.class);
    private int queryTimeout;
    private final DocumentDbQueryExecutor queryExecutor;

    /**
     * DocumentDbStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     *
     * @param connection the connection.
     * @throws SQLException if unable to construct a new {@link java.sql.Statement}.
     */
    DocumentDbStatement(
            final DocumentDbConnection connection) throws SQLException {
        super(connection);
        setDefaultFetchSize(this, connection.getConnectionProperties());
        final DocumentDbQueryMappingService mappingService = new DocumentDbQueryMappingService(
                connection.getConnectionProperties(),
                connection.getDatabaseMetadata());
        queryExecutor = new DocumentDbQueryExecutor(
                this,
                connection.getConnectionProperties(),
                mappingService,
                getQueryTimeout(),
                getFetchSize());
    }

    /**
     * Sets the default fetch size on the {@link java.sql.Statement} object.
     *
     * @param statement the Statement to set.
     * @param properties the
     * @throws SQLException if unable to set the fetch size.
     */
    static void setDefaultFetchSize(
            final java.sql.Statement statement,
            final DocumentDbConnectionProperties properties) throws SQLException {
        Integer defaultFetchSize = properties.getDefaultFetchSize();
        if (defaultFetchSize == null) {
            defaultFetchSize = FETCH_SIZE_DEFAULT;
        }
        if (defaultFetchSize != FETCH_SIZE_DEFAULT) {
            LOGGER.debug("Setting custom default fetch size: {}", defaultFetchSize);
        }
        statement.setFetchSize(defaultFetchSize);
    }

    /**
     * DocumentDbStatement constructor. Accepts a DocumentDbQueryExecutor that can
     * be used for testing purposes.
     * @param connection the connection.
     * @param queryExecutor the DocumentDbQueryExecutor.
     */
    DocumentDbStatement(final DocumentDbConnection connection, final DocumentDbQueryExecutor queryExecutor) {
        super(connection);
        this.queryExecutor = queryExecutor;
    }

    @Override
    protected void cancelQuery(final boolean isClosing) throws SQLException {
        queryExecutor.cancelQuery(isClosing);
    }

    @Override
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        verifyOpen();
        queryExecutor.setFetchSize(getFetchSize());
        return queryExecutor.executeQuery(sql);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        queryTimeout = seconds;
        queryExecutor.setQueryTimeout(seconds);
    }
}
