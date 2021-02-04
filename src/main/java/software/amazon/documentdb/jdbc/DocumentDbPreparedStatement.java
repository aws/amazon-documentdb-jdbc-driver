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

package software.amazon.documentdb.jdbc;

import software.amazon.documentdb.jdbc.common.PreparedStatement;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * DocumentDb implementation of PreparedStatement.
 */
public class DocumentDbPreparedStatement extends PreparedStatement
        implements java.sql.PreparedStatement {
    private final java.sql.PreparedStatement preparedStatement;

    /**
     * DocumentDbPreparedStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param preparedStatement Connection Object.
     * @param sql Sql query.
     */
    public DocumentDbPreparedStatement(final java.sql.PreparedStatement preparedStatement,
            final String sql) throws SQLException {
        super(preparedStatement.getConnection(), sql);
        this.preparedStatement = preparedStatement;
    }


    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        preparedStatement.cancel();
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return Integer.MAX_VALUE;
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        verifyOpen();
        return preparedStatement.executeQuery(getSql());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        verifyOpen();
        return preparedStatement.getMetaData();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return preparedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        preparedStatement.setQueryTimeout(seconds);
    }
}
