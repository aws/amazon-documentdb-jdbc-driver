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

import software.amazon.documentdb.jdbc.common.Statement;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.SQLException;

/**
 * DocumentDb implementation of DatabaseMetadata.
 */
class DocumentDbStatement extends Statement implements java.sql.Statement {
    private int queryTimeout;
    private final DocumentDbQueryExecutor queryExecutor;

    /**
     * DocumentDbStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     *
     * @param connection the connection.
     */
    DocumentDbStatement(
            final DocumentDbConnection connection) throws SQLException {
        super(connection);
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
    protected void cancelQuery() throws SQLException {
        queryExecutor.cancelQuery();
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
