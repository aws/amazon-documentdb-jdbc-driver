/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * DocumentDb implementation of PreparedStatement.
 */
public class DocumentDbPreparedStatement extends software.amazon.jdbc.PreparedStatement
        implements java.sql.PreparedStatement {
    private final DocumentDbQueryExecutor documentDbQueryExecutor;
    private final String sql;
    private java.sql.ResultSet resultSet;

    /**
     * DocumentDbPreparedStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param connection Connection Object.
     * @param sql Sql query.
     */
    public DocumentDbPreparedStatement(final Connection connection, final String sql) {
        super(connection, sql);
        this.sql = sql;
        documentDbQueryExecutor = new DocumentDbQueryExecutor(this, "uri");
        resultSet = null;
    }


    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        documentDbQueryExecutor.cancelQuery();
        // TODO: Async query execution and cancellation.
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return documentDbQueryExecutor.getMaxFetchSize();
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        resultSet = documentDbQueryExecutor.executeQuery(sql);
        return resultSet;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return (resultSet == null) ? null : resultSet.getMetaData();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return documentDbQueryExecutor.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        documentDbQueryExecutor.setQueryTimeout(seconds);
    }
}
