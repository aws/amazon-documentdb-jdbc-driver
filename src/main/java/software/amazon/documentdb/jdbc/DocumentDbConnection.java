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
import com.jcraft.jsch.Session;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.SneakyThrows;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Connection;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NEW;

/**
 * DocumentDb implementation of Connection.
 */
public class DocumentDbConnection extends Connection
        implements java.sql.Connection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnection.class.getName());

    private final DocumentDbConnectionProperties connectionProperties;
    private DocumentDbDatabaseMetaData metadata;
    private DocumentDbDatabaseSchemaMetadata databaseMetadata;
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;
    private Session session;

    /**
     * DocumentDbConnection constructor, initializes super class.
     */
    DocumentDbConnection(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        super(connectionProperties);
        this.connectionProperties = connectionProperties;
        this.session = initializeSshTunnel(connectionProperties);
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
    public static Session initializeSshTunnel(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        try {
            // Example:
            //ssh -f -i ~/.ssh/docdb-sshtunnel.pem -N -L 27019:sample-cluster.cluster-clq6grcezxsp.us-east-1.docdb.amazonaws.com:27017 ec2-user@ec2-54-87-111-63.compute-1.amazonaws.com

            final JSch jSch = new JSch();

            // ********** Add identity for private key. **********
            // sshPrivateKey=<privateKeyFileLocation>, sshPrivateKeyPassphrase=<passphrase>
            // Add support for using `~` to indicate the home directory.
            final String privateKey = "C:\\Users\\bruce.irschick\\secrets\\docdb-sshtunnel.pem";
            // If passPhrase protected, will need to provide this, too.
            final String passPhrase = null;
            jSch.addIdentity(privateKey, passPhrase);

            // ********** Create an SSH session. **********
            // sshUsername=<ssh-username>, sshHostname=<ssh-hostname>, sshPort=<ssh-port> [22]
            // Typically, this can be defaulted to 22
            final int sshPort = 22;
            // Need the user to provide the next three values where sshUsername and sshHost could
            // be combined as <sshUsername>@<sshHost>
            final String sshUsername = "ec2-user";
            final String sshHost = "ec2-54-87-111-63.compute-1.amazonaws.com";
            final Session session = jSch.getSession(sshUsername, sshHost, sshPort);

            // ********** Connect the session. **********
            final boolean strictHostKeyChecking = false;
            if (!strictHostKeyChecking) {
                // This side-steps the issue of populating the `known_hosts` file.
                // sshStrictHostKeyChecking=<yes|no> [yes]
                session.setConfig("StrictHostKeyChecking", "no");
            } else {
                // This could be defaulted to `~/.ssh/known_hosts`.
                // sshKnownHostsFilename=<ssh-known-hosts> [~/.ssh/known_hosts]
                final String knowHostsFilename = "C:\\Users\\bruce.irschick\\.ssh\\known_hosts";
                jSch.setKnownHosts(knowHostsFilename);
                // The default behaviour of `ssh-keyscan` is to hash known hosts and use
                // algorithm `ecdsa-sha2-nistp256`.
                session.setConfig("HashKnownHosts", "yes");
                // TODO: Need to scan the known_hosts files to determine how the host is encoded.
                session.setConfig("server_host_key", "ecdsa-sha2-nistp256");
            }
            session.connect();

            // ********** Setup port forwarding. **********
            // Can take these next two from the connection.getHostname()
            final String clusterHost = "sample-cluster.cluster-clq6grcezxsp.us-east-1.docdb.amazonaws.com";
            final int remoteForwardPort = 27017;
            // This port number could be allocated (pass 0) instead of supplied or hard-coded to be
            // the same as the remote port number.
            final int localForwardPort = 27019;
            session.setPortForwardingL(
                    localForwardPort,
                    clusterHost,
                    remoteForwardPort);

            // ********** Return the session. **********
            return session;
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
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
            session.disconnect();
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
                .buildMongoClientSettings();

        mongoClient = MongoClients.create(settings);
        mongoDatabase = mongoClient.getDatabase(connectionProperties.getDatabase());
        pingDatabase();
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
}
