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

package software.amazon.documentdb.jdbc.common;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.ConnectionProperty;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.common.utilities.Warning;
import java.sql.Array;
import java.sql.Blob;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract implementation of Connection for JDBC Driver.
 */
public abstract class Connection implements java.sql.Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
    private final Properties connectionProperties;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private Map<String, Class<?>> typeMap = new HashMap<>();
    private SQLWarning warnings = null;

    protected Connection(@NonNull final Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    /*
        Functions that have their implementation in this Connection class.
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        verifyOpen();
        final Properties clientInfo = new Properties();
        clientInfo.putAll(connectionProperties);
        clientInfo.putIfAbsent(
                ConnectionProperty.APPLICATION_NAME,
                Driver.APPLICATION_NAME);
        return clientInfo;
    }

    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        throwIfIsClosed(properties);

        connectionProperties.clear();
        if (properties != null) {
            for (final String name : properties.stringPropertyNames()) {
                final String value = properties.getProperty(name);
                setClientInfo(name, value);
            }
        }
        LOGGER.debug("Successfully set client info with all properties.");
    }

    @Override
    public String getClientInfo(final String name) throws SQLException {
        verifyOpen();
        if (name == null) {
            LOGGER.debug("Null value is passed as name, falling back to get client info with null.");
            return null;
        }
        connectionProperties.putIfAbsent(
                ConnectionProperty.APPLICATION_NAME,
                Driver.APPLICATION_NAME);
        return connectionProperties.getProperty(name);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        verifyOpen();
        return typeMap;
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        verifyOpen();
        if (map == null) {
            LOGGER.debug("Null value is passed as conversion map, failing back to an empty hash map.");
            typeMap = new HashMap<>();
        } else {
            typeMap = map;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return (null != iface) && iface.isAssignableFrom(this.getClass());
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        verifyOpen();
        return sql;
    }

    @Override
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
        Objects.requireNonNull(name);
        throwIfIsClosed(null);

        if (isSupportedProperty(name)) {
            if (value != null) {
                connectionProperties.put(name, value);
                LOGGER.debug("Successfully set client info with name {{}} and value {{}}", name, value);
            } else {
                connectionProperties.remove(name);
                LOGGER.debug("Successfully removed client info with name {{}}", name);
            }
        } else {
            addWarning(new SQLWarning(Warning.lookup(Warning.UNSUPPORTED_PROPERTY, name)));
        }
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        }
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.CANNOT_UNWRAP,
                iface.toString());
    }

    @Override
    public void clearWarnings() throws SQLException {
        verifyOpen();
        warnings = null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        verifyOpen();
        return warnings;
    }

    /**
     * Set a new warning if there were none, or add a new warning to the end of the list.
     * @param warning the {@link SQLWarning} to be set.SQLError
     */
    protected void addWarning(final SQLWarning warning) {
        LOGGER.warn(warning.getMessage());
        if (this.warnings == null) {
            this.warnings = warning;
            return;
        }
        this.warnings.setNextWarning(warning);
    }

    /**
     * Closes the connection and releases resources.
     */
    protected abstract void doClose() throws SQLException;

    @Override
    public void close() throws SQLException {
        if (!isClosed.getAndSet(true)) {
            doClose();
        }
    }

    /**
     * Verify the connection is open.
     * @throws SQLException if the connection is closed.
     */
    protected void verifyOpen() throws SQLException {
        if (isClosed.get()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.CONN_CLOSED);
        }
    }

    // Add default implementation of create functions which throw.
    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        // Only reason to do this is for parameters, if you do not support them then this is a safe implementation.
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.PARAMETERS_NOT_SUPPORTED);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(
                LOGGER,
                SqlError.UNSUPPORTED_TYPE,
                Blob.class.toString());
    }

    @Override
    public Clob createClob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(
                LOGGER,
                SqlError.UNSUPPORTED_TYPE,
                Clob.class.toString());
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(
                LOGGER,
                SqlError.UNSUPPORTED_TYPE,
                NClob.class.toString());
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(
                LOGGER,
                SqlError.UNSUPPORTED_TYPE,
                SQLXML.class.toString());
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        // Even though Arrays are supported, the only reason to create an Array in the application is to pass it as
        // a parameter which is not supported.
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.PARAMETERS_NOT_SUPPORTED);
    }


    // Add default of no schema and no catalog support.
    @Override
    public String getSchema() throws SQLException {
        // No schema support. Return null.
        return null;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        // No schema support. Do nothing.
    }

    @Override
    public String getCatalog() throws SQLException {
        // No catalog support. Return null.
        return null;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        // No catalog support. Do nothing.
    }

    // Add default read-only and autocommit only implementation.
    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        // Fake allowing autoCommit to be turned off, even though transactions are not supported, as some applications
        // turn this off without checking support.
        LOGGER.debug("Transactions are not supported, do nothing for setAutoCommit.");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        if (!readOnly) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.READ_ONLY);
        }
    }

    // Default to forward only with read only concurrency.
    @Override
    public java.sql.Statement createStatement() throws SQLException {
        verifyOpen();
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    // Add default no transaction support statement.
    @Override
    public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        verifyOpen();
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_STATEMENT);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType,
                                                       final int resultSetConcurrency,
                                                       final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_STATEMENT);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int[] columnIndexes)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_STATEMENT);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final String[] columnNames)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_STATEMENT);
    }

    // Add default no callable statement support.
    @Override
    public java.sql.CallableStatement prepareCall(final String sql) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_CALL);
    }

    @Override
    public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType,
                                                  final int resultSetConcurrency)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_CALL);
    }

    @Override
    public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType,
                                                  final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_PREPARE_CALL);
    }

    // Default transactions as unsupported.
    @Override
    public int getTransactionIsolation() throws SQLException {
        return java.sql.Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        verifyOpen();
        if (level != java.sql.Connection.TRANSACTION_NONE) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
        }
    }

    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public void rollback() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public void abort(final Executor executor) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public void commit() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.TRANSACTIONS_NOT_SUPPORTED);
    }

    /**
     * Checks if the property is supported by the driver.
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public abstract boolean isSupportedProperty(final String name);

    private void throwIfIsClosed(final Properties properties) throws SQLClientInfoException {
        if (isClosed.get()) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            if (properties != null) {
                for (final String name : properties.stringPropertyNames()) {
                    failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
                }
            }
            throw SqlError.createSQLClientInfoException(LOGGER, SqlError.CONN_CLOSED, failures);
        }
    }
}

