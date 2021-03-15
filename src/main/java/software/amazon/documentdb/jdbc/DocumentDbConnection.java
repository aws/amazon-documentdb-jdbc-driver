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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * DocumentDb implementation of Connection.
 */
public class DocumentDbConnection extends Connection
        implements java.sql.Connection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnection.class.getName());
    private final java.sql.Connection connection;

    /**
     * DocumentDbConnection constructor, initializes super class.
     */
    public DocumentDbConnection(final java.sql.Connection connection) throws SQLException {
        super(connection.getClientInfo());
        this.connection = connection;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        return connection.isValid(timeout);
    }

    @Override
    public void doClose() throws SQLException {
        connection.close();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new DocumentDbDatabaseMetadata(connection.getMetaData());
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds)
            throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType,
            final int resultSetConcurrency)
            throws SQLException {
        return new DocumentDbStatement(
                connection.createStatement(resultSetType, resultSetConcurrency));
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new DocumentDbPreparedStatement(connection.prepareStatement(sql), sql);
    }

    @Override
    public boolean isSupportedProperty(final String name) {
        return DocumentDbConnectionProperty.isSupportedProperty(name);
    }
}
