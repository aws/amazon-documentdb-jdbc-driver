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
import java.sql.SQLFeatureNotSupportedException;

/**
 * DocumentDb implementation of DatabaseMetadata.
 */
class DocumentDbStatement extends Statement implements java.sql.Statement {
    private int queryTimeout;

    /**
     * DocumentDbStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     *
     * @param connection the connection.
     */
    DocumentDbStatement(
            final DocumentDbConnection connection) {
        super(connection);
    }

    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return  Integer.MAX_VALUE;
    }

    @Override
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        verifyOpen();

        final DocumentDbConnection connection = (DocumentDbConnection)getConnection();
        final DocumentDbQueryMappingService mappingService = new DocumentDbQueryMappingService(
                ((DocumentDbConnection)getConnection()).getConnectionProperties(),
                connection.getDatabaseMetadata());
        final DocumentDbQueryExecutor queryExecutor = new DocumentDbQueryExecutor(
                this,
                null,
                mappingService,
                getQueryTimeout(),
                getMaxFetchSize());
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
    }
}
