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

package software.amazon.documentdb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ConnectionString;
import com.mongodb.ReadPreference;
import org.bson.UuidRepresentation;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public class DocumentDbDriver extends Driver {
    // Note: This class must be marked public for the registration/DeviceManager to work.
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbDriver.class.getName());
    private static final String DOCUMENT_DB_SCHEME = "jdbc:documentdb:";

    /**
     * Registers the driver with the DriverManager.
     * Works as documented here:
     * https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html
     * https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
     * https://www.journaldev.com/29142/how-does-jdbc-connection-actually-work
     * https://www.baeldung.com/java-jdbc-loading-drivers
     */
    static  {
        try {
            DriverManager.registerDriver(new DocumentDbDriver());
        } catch (SQLException e) {
            LOGGER.error(
                    String.format("Unable to register driver %s.",
                            DocumentDbDriver.class.getName()),
                            e);
        }
    }

    @Override
    @Nullable
    public Connection connect(@Nullable final String url, final Properties info) throws SQLException {
        if (!isExpectedJdbcScheme(url)) {
            return null;
        }

        try {
            // Let MongoDB driver check the properties and options of the URL.
            setPropertiesFromConnectionString(info, getMongoDbUrl(url));
        } catch (IllegalArgumentException exception) {
            throw new SQLException(exception.getMessage(), exception);
        }

        return new DocumentDbConnection(new DocumentDBConnectionProperties(info));
    }

    @Override
    public boolean acceptsURL(@NonNull final String url) throws SQLException {
        if (url == null) {
            throw new SQLException("The url cannot be null");
        }

        return isExpectedJdbcScheme(url);
    }

    @VisibleForTesting
    static void setPropertiesFromConnectionString(final Properties info, final String mongoDbUrl)
            throws IllegalArgumentException, SQLException {
        // This can throw IllegalArgumentException
        final ConnectionString connectionString = new ConnectionString(mongoDbUrl);

        validateDocumentDbProperties(connectionString);

        addPropertyIfNotSet(info, DocumentDbConnectionProperty.USER.getName(),
                connectionString.getUsername());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.PASSWORD.getName(),
                connectionString.getPassword());
        // Ignore multiple hosts, if provided. Note: this may include optional port number.
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.HOSTNAME.getName(),
                connectionString.getHosts().get(0));
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.DATABASE.getName(),
                connectionString.getDatabase());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.READ_PREFERENCE.getName(),
                connectionString.getReadPreference());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.APPLICATION_NAME.getName(),
                connectionString.getApplicationName());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.REPLICA_SET.getName(),
                connectionString.getRequiredReplicaSetName());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.SERVER_SELECTION_TIMEOUT_MS.getName(),
                connectionString.getServerSelectionTimeout());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.LOCAL_THRESHOLD_MS.getName(),
                connectionString.getLocalThreshold());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.HEARTBEAT_FREQUENCY_MS.getName(),
                connectionString.getHeartbeatFrequency());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.TLS_ENABLED.getName(),
                connectionString.getSslEnabled());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getName(),
                connectionString.getSslInvalidHostnameAllowed());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.CONNECT_TIMEOUT_MS.getName(),
                connectionString.getConnectTimeout());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.SOCKET_TIMEOUT_MS.getName(),
                connectionString.getSocketTimeout());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.MAX_POOL_SIZE.getName(),
                connectionString.getMaxConnectionPoolSize());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.MIN_POOL_SIZE.getName(),
                connectionString.getMinConnectionPoolSize());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.WAIT_QUEUE_TIMEOUT_MS.getName(),
                connectionString.getMaxWaitTime());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.MAX_IDLE_TIME_MS.getName(),
                connectionString.getMaxConnectionIdleTime());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.MAX_LIFE_TIME_MS.getName(),
                connectionString.getMaxConnectionLifeTime());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName(),
                connectionString.getRetryReads());
        addPropertyIfNotSet(info, DocumentDbConnectionProperty.UUID_REPRESENTATION.getName(),
                connectionString.getUuidRepresentation());
    }

    static void validateDocumentDbProperties(final ConnectionString connectionString)
            throws SQLException {
        if (connectionString.getUsername() == null || connectionString.getPassword() == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_USER_PASSWORD);
        }
        if (connectionString.getDatabase() == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_DATABASE);
        }
        if (connectionString.getHosts().size() != 1) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.UNSUPPORTED_MULTIPLE_HOSTS);
        }
        final String replicaSet = connectionString.getRequiredReplicaSetName();
        final String replicaSetDefault = DocumentDbConnectionProperty.REPLICA_SET.getDefaultValue();
        if (replicaSet != null && replicaSet != replicaSetDefault) {
            LOGGER.warn(String.format("DocumentDB may not support replica set '%s'.", replicaSet));
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final String value) {
        if (!isNullOrWhitespace(value)) {
            info.putIfAbsent(key, value);
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final Integer value) {
        if (value != null) {
            info.putIfAbsent(key, value.toString());
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final Boolean value) {
        if (value != null) {
            info.putIfAbsent(key, value.toString());
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final UuidRepresentation value) {
        if (value != null) {
            info.putIfAbsent(key, value.toString().toLowerCase());
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final char[] value) {
        if (value != null) {
            info.putIfAbsent(key, new String(value));
        }
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final ReadPreference value) {
        if (value != null) {
            info.putIfAbsent(key, value.getName());
        }
    }

    private boolean isExpectedJdbcScheme(@Nullable final String url) {
        return url != null && url.startsWith(DOCUMENT_DB_SCHEME);
    }

    @NonNull
    private String getMongoDbUrl(@NonNull final String url) {
        return url.substring(5).replaceFirst("^documentdb:", "mongodb:");
    }

    private static boolean isNullOrWhitespace(@Nullable final String value) {
        return value == null || Pattern.matches("^\\s*$", value);
    }
}
