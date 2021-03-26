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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.DataSource;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import javax.sql.PooledConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * DocumentDb implementation of DataSource.
 */
public class DocumentDbDataSource extends DataSource {
    private final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbDriver.class.getName());

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        properties.validateRequiredProperties();
        return DriverManager.getConnection(DocumentDbDriver.DOCUMENT_DB_SCHEME, properties);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        setUser(username);
        setPassword(password);
        properties.validateRequiredProperties();
        return DriverManager.getConnection(DocumentDbDriver.DOCUMENT_DB_SCHEME, properties);
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @param seconds The connection timeout in seconds.
     *
     * @throws SQLException if timeout is negative.
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        if (seconds < 0) {
            throwInvalidTimeoutException(seconds);
        }
        properties.setLoginTimeout(String.valueOf(seconds));
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @return the connection timeout in seconds.
     */
    @Override
    public int getLoginTimeout() {
        return properties.getLoginTimeout();
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new DocumentDbPooledConnection(getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new DocumentDbPooledConnection(getConnection(user, password));
    }

    /**
     * Sets the username for connection to DocumentDb.
     *
     * @param username The username to authenticate with.
     */
    public void setUser(final String username) {
        properties.setUser(username);
    }

    /**
     * Gets the username.
     *
     * @return The username to authenticate with.
     */
    public String getUser() {
        return properties.getUser();
    }

    /**
     * Sets the password for connection to DocumentDb.
     *
     * @param password The password to authenticate with.
     */
    public void setPassword(final String password) {
        properties.setPassword(password);
    }

    /**
     * Gets the password.
     *
     * @return The password to authenticate with.
     */
    public String getPassword() {
        return properties.getPassword();
    }

    /**
     * Sets the database name.
     *
     * @param database The name of the database.
     */
    public void setDatabase(final String database) {
        properties.setDatabase(database);
    }

    /**
     * Gets the database name.
     *
     * @return The database to connect to.
     */
    public String getDatabase() {
        return properties.getDatabase();
    }

    /**
     * Sets the host name.
     *
     * @param hostname The hostname to connect to.
     */
    public void setHostname(final String hostname) {
        properties.setHostname(hostname);
    }

    /**
     * Gets the hostname.
     *
     * @return The database to connect to.
     */
    public String getHostname() {
        return properties.getHostname();
    }

    /**
     * Sets the read preference when connecting as a replica set.
     *
     * @param readPreference The name of the read preference.
     */
    public void setReadPreference(final DocumentDbReadPreference readPreference) {
        properties.setReadPreference(readPreference.getName());
    }

    /**
     * Gets the read preference.
     *
     * @return The database to connect to.
     */
    public DocumentDbReadPreference getReadPreference() {
        return properties.getReadPreference();
    }

    /**
     * Sets the application name.
     *
     * @param applicationName The name of the application
     */
    public void setApplicationName(final String applicationName) {
        properties.setApplicationName(applicationName);
    }

    /**
     * Gets the application name.
     *
     * @return The name of the application.
     */
    public String getApplicationName() {
        return properties.getApplicationName();
    }

    /**
     * Sets the replica set name.
     *
     * @param replicaSet The name of the replica set to connect to.
     */
    public void setReplicaSet(final String replicaSet) {
        if (replicaSet != null && !replicaSet.equals(DocumentDbConnectionProperty.REPLICA_SET.getDefaultValue())) {
            LOGGER.warn(String.format("DocumentDB may not support replica set '%s'.", replicaSet));
        }
        properties.setReplicaSet(replicaSet);
    }

    /**
     * Gets the replica set name.
     *
     * @return The name of the replica set.
     */
    public String getReplicaSet() {
        return properties.getReplicaSet();
    }

    /**
     * Sets the TLS enabled flag.
     *
     * @param tlsEnabled {@code true} if TLS/SSL is enabled; {@code false} otherwise.
     */
    public void setTlsEnabled(final boolean tlsEnabled) {
        properties.setTlsEnabled(String.valueOf(tlsEnabled));
    }

    /**
     * Gets the TLS enabled flag.
     *
     * @return {@code true} if TLS/SSL is enabled; {@code false} otherwise.
     */
    public boolean getTlsEnabled() {
        return properties.getTlsEnabled();
    }

    /**
     * Sets allow invalid hostnames flag for TLS connections.
     *
     * @param allowInvalidHostnames Whether invalid hostnames are allowed when connecting with
     *                              TLS/SSL.
     */
    public void setTlsAllowInvalidHostnames(final boolean allowInvalidHostnames) {
        properties.setTlsAllowInvalidHostnames(String.valueOf(allowInvalidHostnames));
    }

    /**
     * Gets the allow invalid hostnames flag for TLS connections.
     *
     * @return {@code true} if invalid host names are allowed; {@code false} otherwise.
     */
    public boolean getTlsAllowInvalidHosts() {
        return properties.getTlsAllowInvalidHostnames();
    }

    /**
     * Sets retry reads flag.
     *
     * @param retryReadsEnabled Whether the driver should retry read operations if they fail due to
     *                          a network error
     */
    public void setRetryReadsEnabled(final boolean retryReadsEnabled) {
        properties.setRetryReadsEnabled(String.valueOf(retryReadsEnabled));
    }

    /**
     * Gets the retry reads flag.
     *
     * @return {@code true} if the driver should retry read operations if they fail due to a network
     * error; {@code false} otherwise.
     */
    public boolean getRetryReadsEnabled() {
        return properties.getRetryReadsEnabled();
    }

    private void throwInvalidTimeoutException(final long timeout) throws SQLException {
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.INVALID_TIMEOUT,
                Long.valueOf(timeout)
        );
    }

    @VisibleForTesting
    void validateRequiredProperties() throws SQLException {
        properties.validateRequiredProperties();
    }
}
