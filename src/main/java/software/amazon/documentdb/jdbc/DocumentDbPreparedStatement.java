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

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import software.amazon.documentdb.jdbc.common.PreparedStatement;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * DocumentDb implementation of PreparedStatement.
 */
public class DocumentDbPreparedStatement extends PreparedStatement
        implements java.sql.PreparedStatement {
    private int queryTimeout = 0;
    private final DocumentDbQueryExecutor queryExecutor;

    /**
     * DocumentDbPreparedStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param connection Connection Object.
     * @param sql Sql query.
     */
    public DocumentDbPreparedStatement(final Connection connection, final String sql) throws SQLException {
        super(connection, sql);
        final DocumentDbQueryMappingService mappingService = new DocumentDbQueryMappingService(
                ((DocumentDbConnection) connection).getConnectionProperties(),
                ((DocumentDbConnection) connection).getDatabaseMetadata());
        queryExecutor = new DocumentDbQueryExecutor(
                this,
                ((DocumentDbConnection) connection).getConnectionProperties(),
                mappingService,
                getQueryTimeout(),
                getFetchSize());
    }

    @Override
    protected void cancelQuery() throws SQLException {
        queryExecutor.cancelQuery();
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        verifyOpen();
        return queryExecutor.executeQuery(getSql());
    }

    @Override
    @SneakyThrows
    public ResultSetMetaData getMetaData() throws SQLException {
        verifyOpen();
        if (getResultSet() == null) {
            final DocumentDbConnection connection = (DocumentDbConnection)getConnection();
            final DocumentDbQueryMappingService mappingService = new DocumentDbQueryMappingService(
                    connection.getConnectionProperties(),
                    connection.getDatabaseMetadata());
            return new DocumentDbResultSetMetaData(ImmutableList.copyOf(mappingService.get(getSql()).getColumnMetaData()));
        }
        return getResultSet().getMetaData();
    }

    /**
     * Returns the query timeout setting, with a default value of zero indicating no time limit.
     *
     * @return the query timeout in seconds.
     * @throws SQLException If the statement is closed.
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return queryTimeout;
    }

    /**
     * Sets the time limit for querying. A timeout of zero results in no time limit when querying.
     *
     * @param seconds The query timeout in seconds
     * @throws SQLException If the statement is closed.
     */
    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        queryTimeout = seconds;
        queryExecutor.setQueryTimeout(seconds);
    }
}
