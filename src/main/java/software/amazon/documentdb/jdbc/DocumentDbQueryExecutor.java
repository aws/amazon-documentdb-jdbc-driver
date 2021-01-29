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

import java.sql.SQLException;

/**
 * DocumentDb implementation of QueryExecution.
 */
public class DocumentDbQueryExecutor {
    private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    private final java.sql.Statement statement;
    private final int fetchSize = -1;
    private final String uri;
    private int queryTimeout = -1;

    /**
     * DocumentDbQueryExecutor constructor.
     * @param statement java.sql.Statement Object.
     * @param uri Endpoint to execute queries against.
     */
    DocumentDbQueryExecutor(final java.sql.Statement statement, final String uri) {
        this.uri = uri;
        this.statement = statement;
        // TODO: Add way of getting and setting connection properties here.
    }

    protected void cancelQuery() throws SQLException {
        // TODO: Cancel logic.
    }

    protected int getMaxFetchSize() throws SQLException {
        return MAX_FETCH_SIZE;
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @return java.sql.ResultSet object returned from query execution.
     */
    public java.sql.ResultSet executeQuery(final String sql) {
        // TODO: Throw exception?
        throw new UnsupportedOperationException();
    }

    /**
     * Get query execution timeout in seconds.
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set query execution timeout to the timeout in seconds.
     * @param seconds Time in seconds to set query timeout to.
     */
    public void setQueryTimeout(final int seconds) {
        queryTimeout = seconds;
    }
}
