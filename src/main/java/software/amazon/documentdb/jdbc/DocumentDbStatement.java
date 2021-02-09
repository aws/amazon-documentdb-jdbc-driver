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

import java.sql.SQLException;

/**
 * DocumentDb implementation of DatabaseMetadata.
 */
class DocumentDbStatement extends Statement implements java.sql.Statement {
    private final java.sql.Statement statement;

    /**
     * DocumentDbStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param statement Statement Object.
     */
    public DocumentDbStatement(final java.sql.Statement statement) throws SQLException {
        super(statement.getConnection());
        this.statement = statement;
    }


    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        statement.cancel();
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return  Integer.MAX_VALUE;
    }

    @Override
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        verifyOpen();
        return new DocumentDbResultSet(statement.executeQuery(sql));
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        statement.setQueryTimeout(seconds);
    }
}
