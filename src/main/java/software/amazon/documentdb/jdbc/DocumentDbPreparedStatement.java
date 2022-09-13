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

import static software.amazon.documentdb.jdbc.DocumentDbStatement.setDefaultFetchSize;

/**
 * DocumentDb implementation of PreparedStatement.
 */
public class DocumentDbPreparedStatement extends PreparedStatement
        implements java.sql.PreparedStatement {
    private int queryTimeout = 0;
    private DocumentDbAllowDiskUseOption allowDiskUse = DocumentDbAllowDiskUseOption.DEFAULT;
    private final DocumentDbQueryExecutor queryExecutor;

    /**
     * DocumentDbPreparedStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param connection Connection Object.
     * @param sql Sql query.
     * @throws SQLException if unable to construct a new {@link java.sql.PreparedStatement}.
     */
    public DocumentDbPreparedStatement(final Connection connection, final String sql) throws SQLException {
        super(connection, sql);
        final DocumentDbConnection documentDbConnection = (DocumentDbConnection)getConnection();
        setDefaultFetchSize(this, documentDbConnection.getConnectionProperties());
        final DocumentDbConnectionProperties connectionProperties = documentDbConnection
                .getConnectionProperties();
        final DocumentDbQueryMappingService mappingService = new DocumentDbQueryMappingService(
                connectionProperties,
                documentDbConnection.getDatabaseMetadata());
        setAllowDiskUse(connectionProperties.getAllowDiskUseOption());
        queryExecutor = new DocumentDbQueryExecutor(
                this,
                connectionProperties,
                mappingService,
                getQueryTimeout(),
                getFetchSize(),
                getAllowDiskUse());
    }

    @Override
    protected void cancelQuery(final boolean isClosing) throws SQLException {
        queryExecutor.cancelQuery(isClosing);
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        verifyOpen();
        queryExecutor.setFetchSize(getFetchSize());
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

    /**
     * Gets the allow disk use option for the statement.
     *
     * @return one of the allow disk use options.
     * @throws SQLException if the connection is not open.
     */
    public DocumentDbAllowDiskUseOption getAllowDiskUse() throws SQLException {
        verifyOpen();
        return allowDiskUse;
    }

    /**
     * Sets the allow disk use indicator for the statement.
     *
     * @param allowDiskUse the indicator of whether to set the allow disk use option.
     * @throws SQLException if the connection is not open.
     */
    public void setAllowDiskUse(final DocumentDbAllowDiskUseOption allowDiskUse) throws SQLException {
        verifyOpen();
        this.allowDiskUse = allowDiskUse;
    }
}
