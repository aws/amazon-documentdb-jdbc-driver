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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Connection;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPath;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NEW;

/**
 * DocumentDb implementation of Connection.
 */
public class DocumentDbConnection extends Connection
        implements java.sql.Connection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnection.class.getName());
    public static final String SSH_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts";
    public static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String HASH_KNOWN_HOSTS = "HashKnownHosts";
    public static final String SERVER_HOST_KEY = "server_host_key";
    public static final String ECDSA_SHA_2_NISTP_256 = "ecdsa-sha2-nistp256";
    public static final String YES = "yes";
    public static final String NO = "no";
    public static final String LOCALHOST = "localhost";

    private final DocumentDbConnectionProperties connectionProperties;
    private DocumentDbDatabaseMetaData metadata;
    private DocumentDbDatabaseSchemaMetadata databaseMetadata;
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;
    private SshPortForwardingSession session;

    /**
     * DocumentDbConnection constructor, initializes super class.
     */
    DocumentDbConnection(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        super(connectionProperties);
        this.connectionProperties = connectionProperties;
        if (LOGGER.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Creating connection with following properties:");
            for (String propertyName : connectionProperties.stringPropertyNames()) {
                if (!DocumentDbConnectionProperty.PASSWORD.getName().equals(propertyName)) {
                    sb.append(String.format("%n        Connection property %s=%s",
                            propertyName, connectionProperties.get(propertyName).toString()));
                }
            }
            LOGGER.debug(sb.toString());
        }
        this.session = createSshTunnel(connectionProperties);
        initializeClients(connectionProperties);
    }

    /**
     * Initializes the SSH session and creates a port forwarding tunnel.
     *
     * @param connectionProperties the {@link DocumentDbConnectionProperties} connection properties.
     * @return a {@link Session} session. This session must be closed by calling the
     *          {@link Session#disconnect()} method.
     * @throws SQLException if unable to create SSH session or create the port forwarding tunnel.
     */
    public static SshPortForwardingSession createSshTunnel(
            final DocumentDbConnectionProperties connectionProperties) throws SQLException {
        if (!connectionProperties.enableSshTunnel()) {
            return null;
        }

        try {
            final JSch jSch = new JSch();
            addIdentity(connectionProperties, jSch);
            final Session session = createSession(connectionProperties, jSch);
            connectSession(connectionProperties, jSch, session);
            return getPortForwardingSession(connectionProperties, session);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static SshPortForwardingSession getPortForwardingSession(
            final DocumentDbConnectionProperties connectionProperties,
            final Session session) throws JSchException {
        final int portSeparatorIndex = connectionProperties.getHostname().indexOf(':');
        final String clusterHost;
        final int clusterPort;
        if (portSeparatorIndex >= 0) {
            clusterHost = connectionProperties.getHostname().substring(0, portSeparatorIndex);
            clusterPort = Integer.parseInt(
                    connectionProperties.getHostname().substring(portSeparatorIndex + 1));
        } else {
            clusterHost = connectionProperties.getHostname();
            clusterPort = 27017;
        }
        final int localPort = session.setPortForwardingL(LOCALHOST, 0, clusterHost, clusterPort);
        return new SshPortForwardingSession(session, localPort);
    }

    private static void connectSession(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch,
            final Session session) throws JSchException {
        setSecurityConfig(connectionProperties, jSch, session);
        session.connect();
    }

    private static void addIdentity(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch) throws JSchException {
        final String privateKey = getPath(connectionProperties.getSshPrivateKeyFile()).toString();
        // If passPhrase protected, will need to provide this, too.
        final String passPhrase = isNullOrWhitespace(connectionProperties.getSshPrivateKeyPassphrase())
                ? connectionProperties.getSshPrivateKeyPassphrase()
                : null;
        jSch.addIdentity(privateKey, passPhrase);
    }

    private static Session createSession(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch) throws JSchException {
        final String sshUsername = connectionProperties.getSshUser();
        final String sshHostname;
        final int sshPort;
        final int portSeparatorIndex = connectionProperties.getSshHostname().indexOf(':');
        if (portSeparatorIndex >= 0) {
            sshHostname = connectionProperties.getSshHostname().substring(0, portSeparatorIndex);
            sshPort = Integer.parseInt(
                    connectionProperties.getSshHostname().substring(portSeparatorIndex + 1));
        } else {
            sshHostname = connectionProperties.getSshHostname();
            sshPort = 22;
        }
        return jSch.getSession(sshUsername, sshHostname, sshPort);
    }

    private static void setSecurityConfig(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch,
            final Session session) throws JSchException {

        if (!connectionProperties.getSshStrictHostKeyChecking()) {
            session.setConfig(STRICT_HOST_KEY_CHECKING, NO);
            return;
        }

        // This could be defaulted to `~/.ssh/known_hosts`.
        // sshKnownHostsFilename=<ssh-known-hosts> [~/.ssh/known_hosts]
        final String knowHostsFilename;
        if (isNullOrWhitespace(connectionProperties.getSshKnownHostsFile())) {
            knowHostsFilename = getPath(SSH_KNOWN_HOSTS_FILE)
                    .toAbsolutePath().toString();
        } else {
            final Path knownHostsPath = getPath(connectionProperties.getSshKnownHostsFile())
                    .toAbsolutePath();
            if (Files.exists(knownHostsPath)) {
                knowHostsFilename = knownHostsPath.toString();
            } else {
                knowHostsFilename = getPath(SSH_KNOWN_HOSTS_FILE)
                        .toAbsolutePath().toString();
            }
        }
        jSch.setKnownHosts(knowHostsFilename);
        // The default behaviour of `ssh-keyscan` is to hash known hosts and use
        // algorithm `ecdsa-sha2-nistp256`.
        session.setConfig(HASH_KNOWN_HOSTS, YES);
        // TODO: Need to scan the known_hosts files to determine how the host is encoded.
        session.setConfig(SERVER_HOST_KEY, ECDSA_SHA_2_NISTP_256);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.INVALID_TIMEOUT,
                    timeout);
        }
        if (mongoDatabase != null) {
            try {
                // Convert to milliseconds
                final int maxTimeMS = timeout + 1000;
                pingDatabase(maxTimeMS);
                return true;
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public void doClose() {
        if (mongoDatabase != null) {
            mongoDatabase = null;
        }
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
        if (session != null) {
            session.session.disconnect();
            session = null;
        }
    }

    @SneakyThrows
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureDatabaseMetadata();
        return metadata;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    private void ensureDatabaseMetadata() throws SQLException {
        if (metadata == null) {
            databaseMetadata = DocumentDbDatabaseSchemaMetadata.get(
                    connectionProperties, connectionProperties.getSchemaName(), VERSION_LATEST_OR_NEW);
            metadata = new DocumentDbDatabaseMetaData(this, databaseMetadata, connectionProperties);
        }
    }

    DocumentDbDatabaseSchemaMetadata getDatabaseMetadata()
            throws SQLException {
        ensureDatabaseMetadata();
        return databaseMetadata;
    }

    @Override
    public String getSchema() {
        return connectionProperties.getDatabase();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO: Implement network timeout.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds)
            throws SQLException {
        // TODO: Implement network timeout.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType,
                                              final int resultSetConcurrency)
            throws SQLException {

        verifyOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }

        return new DocumentDbStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        verifyOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        return new DocumentDbPreparedStatement(this, sql);
    }

    @Override
    public boolean isSupportedProperty(final String name) {
        return DocumentDbConnectionProperty.isSupportedProperty(name);
    }

    DocumentDbConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    private void initializeClients(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        // Create the mongo client.
        final MongoClientSettings settings = connectionProperties
                .buildMongoClientSettings(getSshLocalPort());

        mongoClient = MongoClients.create(settings);
        mongoDatabase = mongoClient.getDatabase(connectionProperties.getDatabase());
        pingDatabase();
    }

    private int getSshLocalPort() {
        // Get the port from the SSH tunnel session, if it exists.
        if (session != null) {
            return session.localPort;
        }
        return 0;
    }

    private void pingDatabase() throws SQLException {
        pingDatabase(0);
    }

    private void pingDatabase(final int maxTimeMS) throws SQLException {
        try {
            final String maxTimeMSOption = (maxTimeMS > 0)
                    ? String.format(", \"maxTimeMS\" : %d", maxTimeMS)
                    : "";
            mongoDatabase.runCommand(
                    Document.parse(String.format("{ \"ping\" : 1 %s }", maxTimeMSOption)));
        } catch (MongoSecurityException e) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    e,
                    SqlError.SECURITY_ERROR,
                    e.getMessage());
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Container for the SSH port forwarding tunnel session.
     */
    @Getter
    @AllArgsConstructor
    public static class SshPortForwardingSession {
        /**
         * Gets the SSH session.
         */
        private Session session;
        /**
         * Gets the local port for the port forwarding tunnel.
         */
        private int localPort;
    }
}
