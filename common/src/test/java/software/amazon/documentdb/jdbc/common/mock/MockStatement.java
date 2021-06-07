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

package software.amazon.documentdb.jdbc.common.mock;

import software.amazon.documentdb.jdbc.common.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mock implementation for Statement object so it can be instantiated and tested.
 */
public class MockStatement extends Statement implements java.sql.Statement {
    private ResultSet resultSet = null;

    /**
     * Constructor for MockStatement.
     * @param connection Connection to pass to Statement.
     */
    public MockStatement(final Connection connection) {
        super(connection);
    }

    public void setResultSet(final ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    protected void cancelQuery() throws SQLException {

    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        return resultSet;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {

    }
}
