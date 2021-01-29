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

import software.amazon.documentdb.jdbc.common.Statement;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * DocumentDb implementation of DatabaseMetadata.
 */
public class DocumentDbStatement extends Statement implements java.sql.Statement {
    private final DocumentDbQueryExecutor documentDbQueryExecutor;

    /**
     * DocumentDbStatement constructor, creates DocumentDbQueryExecutor and initializes super class.
     * @param connection Connection Object.
     */
    public DocumentDbStatement(final Connection connection) {
        super(connection);
        documentDbQueryExecutor = new DocumentDbQueryExecutor(this, "uri");
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
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        return documentDbQueryExecutor.executeQuery(sql);
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
