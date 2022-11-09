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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
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
import software.amazon.documentdb.jdbc.sshtunnel.DocumentDbSshTunnelClient;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperty.REFRESH_SCHEMA;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NEW;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

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
    private DocumentDbSshTunnelClient sshTunnelClient;

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
        maybeCreateSshTunnel(connectionProperties);
        initializeClients(connectionProperties);
    }

    private void maybeCreateSshTunnel(final DocumentDbConnectionProperties connectionProperties) throws SQLException {
        if (!connectionProperties.enableSshTunnel()) {
            LOGGER.info("Internal SSH tunnel not started.");
            return;
        }

        try {
            this.sshTunnelClient = new DocumentDbSshTunnelClient(connectionProperties);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw SqlError.createSQLException(LOGGER, SqlState.CONNECTION_EXCEPTION, e,
                    SqlError.SSH_TUNNEL_ERROR, e.getMessage());
        }
    }

    /**
     * Gets the ssh tunnel local port.
     *
     * @return the ssh tunnel local port if it exists; 0 otherwise.
     */
    public int getSshLocalPort() {
        // Get the port from the SSH tunnel session, if it exists.
        if (isSshTunnelActive()) {
            return sshTunnelClient.getServiceListeningPort();
        }
        return 0;
    }

    /**
     * Get whether the SSH tunnel is active.
     *
     * @return returns {@code true} if the SSH tunnel is active, {@code false}, otherwise.
     */
    @SneakyThrows
    public boolean isSshTunnelActive() {
        // indicate whether the SSH tunnel is enabled
        return sshTunnelClient != null && sshTunnelClient.getServiceListeningPort() > 0;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.INVALID_PARAMETER_VALUE,
                    SqlError.INVALID_TIMEOUT,
                    timeout);
        }
        if (mongoDatabase != null) {
            try {
                // Convert to milliseconds
                final long maxTimeMS = TimeUnit.SECONDS.toMillis(timeout);
                pingDatabase(maxTimeMS);
                return true;
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public void doClose() throws SQLException {
        if (mongoDatabase != null) {
            mongoDatabase = null;
        }
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
        if (sshTunnelClient != null) {
            try {
                sshTunnelClient.close();
            } catch (Exception e) {
                throw SqlError.createSQLException(LOGGER, SqlState.CONNECTION_EXCEPTION, e,
                        SqlError.SSH_TUNNEL_ERROR, e.getMessage());
            } finally {
                sshTunnelClient = null;
            }
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
            final int version;
            if (connectionProperties.getRefreshSchema())  {
                version = VERSION_NEW;
                LOGGER.warn("The '{}' option is enabled and will cause a new"
                        + " version of the SQL schema to be generated."
                        + " This can lead to poor performance."
                        + " Please disable this option when it is no longer needed.",
                        REFRESH_SCHEMA.getName());
            } else {
                version = VERSION_LATEST_OR_NEW;
            }
            setMetadata(version);
        }
    }

    private void setMetadata(final int version) throws SQLException {
        databaseMetadata = DocumentDbDatabaseSchemaMetadata.get(
                connectionProperties,
                connectionProperties.getSchemaName(),
                version,
                getMongoClient());
        metadata = new DocumentDbDatabaseMetaData(this, databaseMetadata, connectionProperties);
    }

    void refreshDatabaseMetadata() throws SQLException {
        setMetadata(VERSION_NEW);
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

    private void pingDatabase() throws SQLException {
        pingDatabase(0);
    }

    private void pingDatabase(final long maxTimeMS) throws SQLException {
        try {
            final String maxTimeMSOption = (maxTimeMS > 0)
                    ? String.format(", \"maxTimeMS\" : %d", maxTimeMS)
                    : "";
            mongoDatabase.runCommand(
                    Document.parse(String.format("{ \"ping\" : 1 %s }", maxTimeMSOption)));
        } catch (MongoSecurityException e) {
            // Check specifically for authorization error.
            if (e.getCode() == -4
                    && e.getCause() != null
                    && e.getCause() instanceof MongoCommandException
                    && ((MongoCommandException)e.getCause()).getCode() == 18) {
                throw SqlError.createSQLException(LOGGER,
                        SqlState.INVALID_AUTHORIZATION_SPECIFICATION,
                        e,
                        SqlError.AUTHORIZATION_ERROR,
                        mongoDatabase.getName(),
                        e.getCredential().getUserName(),
                        e.getCredential().getSource(),
                        e.getCredential().getMechanism());
            }
            // Everything else.
            throw SqlError.createSQLException(LOGGER,
                    SqlState.SQL_CLIENT_UNABLE_TO_ESTABLISH_SQL_CONNECTION,
                    e,
                    SqlError.SECURITY_ERROR,
                    e.getMessage());
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }
}
