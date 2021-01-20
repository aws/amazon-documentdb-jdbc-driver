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

package software.amazon.documentdb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DocumentDb implementation of Connection.
 */
public class DocumentDbConnection extends software.amazon.jdbc.Connection
        implements java.sql.Connection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnection.class.getName());
    private static final String AUTHENTICATION_DATABASE = "admin";
    private static final int HEARTBEAT_WAIT_TIME_MS = 1000;

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase = null;
    private DocumentDbServerMonitorListener serverMonitorListener = null;

    /**
     * DocumentDbConnection constructor, initializes super class.
     *
     * @param connectionProperties Properties Object.
     */
    public DocumentDbConnection(@NonNull final DocumentDbConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
        initializeClients(connectionProperties);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlError.createSQLException(
                    LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_TIMEOUT, timeout);
        }

        if (isClosed()) {
            return false;
        }

        final Instant endTime = Instant.now().plus(timeout, ChronoUnit.SECONDS);
        while (!serverMonitorListener.getHasSuccessfulHeartBeat()
                && Instant.now().isBefore(endTime)) {
            try {
                // TODO: Implement a better way to determine the wait time. Currently, set to
                // 1000 ms for all cases so we wait at worst timeout + 1 for a response.
                Thread.sleep(HEARTBEAT_WAIT_TIME_MS);
            } catch (InterruptedException e) {
                return false;
            }
        }

        return serverMonitorListener.getHasSuccessfulHeartBeat();
    }

    @Override
    public void doClose() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            mongoDatabase = null;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() {
        // TODO.
        return null;
    }

    @Override
    public int getNetworkTimeout() {
        // TODO.
        return 0;
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) {
        // TODO.
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType,
            final int resultSetConcurrency)
            throws SQLException {
        return new DocumentDbStatement(this);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new DocumentDbPreparedStatement(this, sql);
    }

    @Override
    public boolean isSupportedProperty(final String name) {
        return Arrays
                .stream(DocumentDbConnectionProperty.values())
                .anyMatch(value -> value.getName().equals(name));
    }

    /**
     * Gets the MongoDatabase for the connection.
     *
     * @return a MongoDatabase for the connection.
     */
    @VisibleForTesting
    protected MongoDatabase getMongoDatabase() throws SQLException {
        return mongoDatabase;
    }

    /**
     * Initializes the objects needed for communication with the DocumentDB service and verifies the
     * connection.
     *
     * @param connectionProperties The connection properties.
     * @throws SQLException if connecting to the given database fails or the database is invalid.
     */
    private void initializeClients(final DocumentDbConnectionProperties connectionProperties) throws SQLException {
        // Create a server monitor listener for the mongo client.
        serverMonitorListener = new DocumentDbServerMonitorListener();

        // Create the mongo client.
        buildMongoClient(connectionProperties, serverMonitorListener);

        // Set the database all operations will be performed on.
        setDatabase(connectionProperties.getDatabase());

        // Verify the connection.
        pingDatabase();
    }

    /**
     * Attempts to ping the database.
     *
     * @throws SQLException if connecting to the database fails for any reason.
     */
    private void pingDatabase() throws SQLException {
        try {
            mongoDatabase.runCommand(new Document("ping", 1));
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Sets the database. Even databases that do not exist yet can be retrieved but invalid names
     * will be rejected.
     *
     * @param database The database to retrieve.
     * @throws SQLException if the database name is invalid.
     */
    private void setDatabase(@NonNull final String database) throws SQLException {
        try {
            mongoDatabase = mongoClient.getDatabase(database);
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Builds the Mongo client from the given connection properties and server monitor listener.
     *
     * @param connectionProperties  The connection properties
     * @param serverMonitorListener The listener for server heartbeat events
     */
    private void buildMongoClient(
            final DocumentDbConnectionProperties connectionProperties,
            final ServerMonitorListener serverMonitorListener) {
        final MongoClientSettings.Builder clientSettingsBuilder = MongoClientSettings.builder();

        // Create credential for admin database (only authentication database in DocumentDB).
        final String user = connectionProperties.getUser();
        final String password = connectionProperties.getPassword();
        if (user != null && password != null) {
            final MongoCredential credential =
                    MongoCredential.createCredential(user, AUTHENTICATION_DATABASE, password.toCharArray());
            clientSettingsBuilder.credential(credential);
        }

        // Set the server configuration.
        applyServerSettings(connectionProperties, clientSettingsBuilder, serverMonitorListener);

        // Set the cluster configuration.
        applyClusterSettings(connectionProperties, clientSettingsBuilder);

        // Set the socket configuration.
        applySocketSettings(connectionProperties, clientSettingsBuilder);

        // Set the connection pool configuration.
        applyConnectionPoolSettings(connectionProperties, clientSettingsBuilder);

        // Set the SSL/TLS configuration.
        applyTlsSettings(connectionProperties, clientSettingsBuilder);

        // Set the UUID representation.
        final UuidRepresentation uuidRepresentation = connectionProperties.getUUIDRepresentation();
        if (uuidRepresentation != null) {
            clientSettingsBuilder.uuidRepresentation(uuidRepresentation);
        }

        // Set the read preference.
        final ReadPreference readPreference = connectionProperties.getReadPreference();
        if (readPreference != null) {
            clientSettingsBuilder.readPreference(readPreference);
        }

        // Get retry reads.
        final boolean retryReads = connectionProperties.getRetryReadsEnabled();

        final MongoClientSettings settings =
                clientSettingsBuilder
                        .applicationName(connectionProperties.getApplicationName())
                        .retryReads(retryReads)
                        .build();

        mongoClient = MongoClients.create(settings);
    }

    /**
     * Applies the server-related connection properties to the given client settings builder.
     *
     * @param connectionProperties The connection properties to use.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     * @param serverMonitorListener The server monitor listener to add as an event listener.
     */
    private static void applyServerSettings(
            final DocumentDbConnectionProperties connectionProperties,
            final MongoClientSettings.Builder clientSettingsBuilder,
            final ServerMonitorListener serverMonitorListener) {
        final Long heartbeatFrequency = connectionProperties.getHeartbeatFrequency();
        clientSettingsBuilder.applyToServerSettings(
                b -> {
                    if (heartbeatFrequency != null) {
                        b.heartbeatFrequency(heartbeatFrequency, TimeUnit.MILLISECONDS);
                    }

                    b.addServerMonitorListener(serverMonitorListener);
                });
    }

    /**
     * Applies the cluster-related connection properties to the given client settings builder.
     * @param connectionProperties The connection properties to use.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private static void applyClusterSettings(
            final DocumentDbConnectionProperties connectionProperties,
            final MongoClientSettings.Builder clientSettingsBuilder) {
        final String host = connectionProperties.getHostname();
        final Long serverSelectionTimeout = connectionProperties.getServerSelectionTimeout();
        final Long localThreshold = connectionProperties.getLocalThreshold();
        final String replicaSetName = connectionProperties.getReplicaSet();

        clientSettingsBuilder.applyToClusterSettings(
                b -> {
                    if (host != null) {
                        b.hosts(Collections.singletonList(new ServerAddress(host)));
                    }

                    if (serverSelectionTimeout != null) {
                        b.localThreshold(serverSelectionTimeout, TimeUnit.MILLISECONDS);
                    }

                    if (localThreshold != null) {
                        b.localThreshold(localThreshold, TimeUnit.MILLISECONDS);
                    }

                    if (replicaSetName != null) {
                        b.requiredReplicaSetName(replicaSetName);
                    }
                });
    }

    /**
     * Applies the socket-related connection properties to the given client settings builder.
     * @param connectionProperties The connection properties to use.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private static void applySocketSettings(
            final DocumentDbConnectionProperties connectionProperties,
            final MongoClientSettings.Builder clientSettingsBuilder) {
        final Integer socketTimeout = connectionProperties.getSocketTimeout();
        final Integer connectTimeout = connectionProperties.getConnectTimeout();

        clientSettingsBuilder.applyToSocketSettings(
                b -> {
                    if (connectTimeout != null) {
                        b.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
                    }

                    if (socketTimeout != null) {
                        b.readTimeout(socketTimeout, TimeUnit.MILLISECONDS);
                    }
                });
    }

    /**
     * Applies the connection-pool-related connection properties to the given client
     * settings builder.
     * @param connectionProperties The connection properties to use.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private static void applyConnectionPoolSettings(
            final DocumentDbConnectionProperties connectionProperties,
            final MongoClientSettings.Builder clientSettingsBuilder) {
        final Integer maxPoolSize = connectionProperties.getMaxPoolSize();
        final Integer minPoolSize = connectionProperties.getMinPoolSize();
        final Integer maxConnectionLifeTime = connectionProperties.getMaxLifeTime();
        final Integer maxConnectionIdleTime = connectionProperties.getMaxIdleTime();
        final Long maxWaitTime = connectionProperties.getWaitQueueTimeout();

        clientSettingsBuilder.applyToConnectionPoolSettings(
                b -> {
                    if (maxPoolSize != null) {
                        b.maxSize(maxPoolSize);
                    }

                    if (minPoolSize != null) {
                        b.minSize(minPoolSize);
                    }

                    if (maxConnectionLifeTime != null) {
                        b.maxConnectionLifeTime(maxConnectionLifeTime, TimeUnit.MILLISECONDS);
                    }

                    if (maxConnectionLifeTime != null) {
                        b.maxConnectionIdleTime(maxConnectionIdleTime, TimeUnit.MILLISECONDS);
                    }

                    if (maxWaitTime != null) {
                        b.maxWaitTime(maxWaitTime, TimeUnit.MILLISECONDS);
                    }
                });
    }

    /**
     * Applies the TLS/SSL-related connection properties to the given client settings builder.
     * @param connectionProperties The connection properties to use.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private static void applyTlsSettings(
            final DocumentDbConnectionProperties connectionProperties,
            final MongoClientSettings.Builder clientSettingsBuilder) {
        final boolean tlsEnabled = connectionProperties.getTlsEnabled();
        final boolean tlsAllowInvalidHostnames = connectionProperties.getTlsAllowInvalidHostnames();
        clientSettingsBuilder.applyToSslSettings(
                b -> b.enabled(tlsEnabled).invalidHostNameAllowed(tlsAllowInvalidHostnames));
    }

    /** Listens to Mongo DB server heartbeat events. */
    private static class DocumentDbServerMonitorListener implements ServerMonitorListener {

        private final AtomicBoolean hasSuccessfulHeartbeat = new AtomicBoolean();

        @Override
        public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
            hasSuccessfulHeartbeat.set(true);
        }

        @Override
        public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
            hasSuccessfulHeartbeat.set(true);
        }

        @Override
        public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
            hasSuccessfulHeartbeat.set(false);
        }

        public boolean getHasSuccessfulHeartBeat() {
            return hasSuccessfulHeartbeat.get();
        }
    }
}
