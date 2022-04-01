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

import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.documentdb.jdbc.common.Connection;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Mock implementation for Connection object so it can be instantiated and tested.
 */
public class MockConnection extends Connection implements java.sql.Connection {

    /**
     * Constructor for MockConnection.
     * @param connectionProperties Properties to pass to Connection.
     */
    public MockConnection(
            final @NonNull Properties connectionProperties) {
        super(connectionProperties);
    }

    @Override
    protected void doClose() {

    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql,
                                              final int resultSetType,
                                              final int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public boolean isSupportedProperty(final String name) {
        return Arrays
                .stream(MockConnectionProperty.values())
                .anyMatch(value -> value.getName().equals(name));
    }
}
