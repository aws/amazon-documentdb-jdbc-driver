package software.amazon.documentdb;

import com.mongodb.ReadPreference;
import org.bson.UuidRepresentation;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class DocumentDbConnectionProperties extends Properties {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnectionProperties.class.getName());

    /**
     * Constructor for DocumentDbConnectionProperties, initializes with given properties.
     *
     * @param properties Properties to initialize with.
     */
    public DocumentDbConnectionProperties(final Properties properties) {
        super(properties);
    }

    /**
     * Constructor for DocumentDbConnectionProperties. Initialized with empty properties.
     */
    public DocumentDbConnectionProperties() {
        super();
    }

    /**
     * Gets the hostname.
     *
     * @return The hostname to connect to.
     */
    public String getHostname() {
        return getProperty(DocumentDbConnectionProperty.HOSTNAME.getName());
    }

    /**
     * Sets the hostname.
     *
     * @param hostname The hostname to connect to.
     */
    public void setHostname(final String hostname) {
        setProperty(DocumentDbConnectionProperty.HOSTNAME.getName(), hostname);
    }

    /**
     * Gets the username.
     *
     * @return The username to authenticate with.
     */
    public String getUser() {
        return getProperty(DocumentDbConnectionProperty.USER.getName());
    }

    /**
     * Sets the user.
     *
     * @param user The username to authenticate with.
     */
    public void setUser(final String user) {
        setProperty(DocumentDbConnectionProperty.USER.getName(), user);
    }

    /**
     * Gets the password.
     *
     * @return The password to authenticate with.
     */
    public String getPassword() {
        return getProperty(DocumentDbConnectionProperty.PASSWORD.getName());
    }

    /**
     * Sets the password.
     *
     * @param password The password to authenticate with.
     */
    public void setPassword(final String password) {
        setProperty(DocumentDbConnectionProperty.PASSWORD.getName(), password);
    }

    /**
     * Gets the database name.
     *
     * @return The database to connect to.
     */
    public String getDatabase() {
        return getProperty(DocumentDbConnectionProperty.DATABASE.getName());
    }

    /**
     * Sets the database name.
     *
     * @param database The database to connect to.
     */
    public void setDatabase(final String database) {
        setProperty(DocumentDbConnectionProperty.DATABASE.getName(), database);
    }

    /**
     * Gets the application name.
     *
     * @return The name of the application.
     */
    public String getApplicationName() {
        return getProperty(DocumentDbConnectionProperty.APPLICATION_NAME.getName());
    }

    /**
     * Sets the application name.
     *
     * @param applicationName The name of the application.
     */
    public void setApplicationName(final String applicationName) {
        setProperty(DocumentDbConnectionProperty.APPLICATION_NAME.getName(), applicationName);
    }

    /**
     * Gets the replica set name.
     *
     * @return The name of the replica set to connect to.
     */
    public String getReplicaSet() {
        return getProperty(DocumentDbConnectionProperty.REPLICA_SET.getName());
    }

    /**
     * Sets the replica set name.
     *
     * @param replicaSet The name of the replica set to connect to.
     */
    public void setReplicaSet(final String replicaSet) {
        setProperty(DocumentDbConnectionProperty.REPLICA_SET.getName(), replicaSet);
    }

    /**
     * Gets TLS enabled flag.
     *
     * @return tlsEnabled {@code true} if TLS/SSL is enabled; {@code false} otherwise..
     */
    public boolean getTlsEnabled() {
        return Boolean.parseBoolean(
                getProperty(
                        DocumentDbConnectionProperty.TLS_ENABLED.getName(),
                        DocumentDbConnectionProperty.TLS_ENABLED.getDefaultValue()));
    }

    /**
     * Sets TLS enabled flag.
     *
     * @param tlsEnabled {@code true} if TLS/SSL is enabled; {@code false} otherwise.
     */
    public void setTlsEnabled(final String tlsEnabled) {
        setProperty(DocumentDbConnectionProperty.TLS_ENABLED.getName(), tlsEnabled);
    }

    /**
     * Gets allow invalid hostnames flag for TLS connections.
     *
     * @return {@code true} if invalid host names are allowed; {@code false} otherwise.
     */
    public boolean getTlsAllowInvalidHostnames() {
        return Boolean.parseBoolean(
                getProperty(
                        DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getName(),
                        DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES
                                .getDefaultValue()));
    }

    /**
     * Sets allow invalid hostnames flag for TLS connections.
     *
     * @param allowInvalidHostnames Whether invalid hostnames are allowed when connecting with
     *     TLS/SSL.
     */
    public void setTlsAllowInvalidHostnames(final String allowInvalidHostnames) {
        setProperty(
                DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getName(),
                allowInvalidHostnames);
    }

    /**
     * Gets retry reads flag.
     *
     * @return {@code true} if the driver should retry read operations if they fail due to a network
     * error; {@code false} otherwise.
     */
    public Boolean getRetryReadsEnabled() {
        return Boolean.parseBoolean(
                getProperty(
                        DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName(),
                        DocumentDbConnectionProperty.RETRY_READS_ENABLED.getDefaultValue()));

    }

    /**
     * Sets retry reads flag.
     *
     * @param retryReadsEnabled Whether the driver should retry read operations if they fail due to
     *                          a network error
     */
    public void setRetryReadsEnabled(final String retryReadsEnabled) {
        setProperty(DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName(), retryReadsEnabled);
    }

    /**
     * Sets server selection timeout.
     *
     * @return The server selection timeout in milliseconds.
     */
    public Long getServerSelectionTimeout() {
        return getPropertyAsLong(
                DocumentDbConnectionProperty.SERVER_SELECTION_TIMEOUT_MS.getName());
    }

    /**
     * Gets server selection timeout.
     *
     * @param timeout The server selection timeout in milliseconds.
     */
    public void setServerSelectionTimeout(final String timeout) {
        setProperty(DocumentDbConnectionProperty.SERVER_SELECTION_TIMEOUT_MS.getName(), timeout);
    }

    /**
     * Gets local threshold for server selection.
     *
     * @return The local threshold in milliseconds.
     */
    public Long getLocalThreshold() {
        return getPropertyAsLong(DocumentDbConnectionProperty.LOCAL_THRESHOLD_MS.getName());
    }

    /**
     * Sets local threshold for server selection.
     *
     * @param threshold The local threshold in milliseconds.
     */
    public void setLocalThreshold(final String threshold) {
        setProperty(DocumentDbConnectionProperty.LOCAL_THRESHOLD_MS.getName(), threshold);
    }

    /**
     * Gets the server heartbeat frequency in milliseconds.
     *
     * @return The heartbeat frequency in milliseconds.
     */
    public Long getHeartbeatFrequency() {
        return getPropertyAsLong(DocumentDbConnectionProperty.HEARTBEAT_FREQUENCY_MS.getName());
    }

    /**
     * Sets server the heartbeat frequency in milliseconds.
     *
     * @param frequency The heartbeat frequency in milliseconds.
     */
    public void setHeartbeatFrequency(final String frequency) {
        setProperty(DocumentDbConnectionProperty.HEARTBEAT_FREQUENCY_MS.getName(), frequency);
    }

    /**
     * Get the timeout for opening a connection.
     *
     * @return The connect timeout in milliseconds.
     */
    public Integer getConnectTimeout() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.CONNECT_TIMEOUT_MS.getName());
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @param timeout The connect timeout in milliseconds.
     */
    public void setConnectTimeout(final String timeout) {
        setProperty(DocumentDbConnectionProperty.CONNECT_TIMEOUT_MS.getName(), timeout);
    }

    /**
     * Gets the timeout for sending or receiving on s socket.
     *
     * @return The socket timeout in milliseconds.
     */
    public Integer getSocketTimeout() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.SOCKET_TIMEOUT_MS.getName());
    }

    /**
     * Sets the timeout for sending or receiving on s socket.
     *
     * @param timeout The socket timeout in milliseconds.
     */
    public void setSocketTimeout(final String timeout) {
        setProperty(DocumentDbConnectionProperty.SOCKET_TIMEOUT_MS.getName(), timeout);
    }

    /**
     * Gets the maximum pool size for pooled connections.
     *
     * @return The maximum pool size for pooled connections.
     */
    public Integer getMaxPoolSize() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.MAX_POOL_SIZE.getName());
    }

    /**
     * Sets the maximum pool size for pooled connections.
     *
     * @param poolSize The maximum pool size for pooled connections.
     */
    public void setMaxPoolSize(final String poolSize) {
        setProperty(DocumentDbConnectionProperty.MAX_POOL_SIZE.getName(), poolSize);
    }

    /**
     * Gets the minimum pool size for pooled connections.
     *
     * @return The minimum pool size for pooled connections.
     */
    public Integer getMinPoolSize() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.MIN_POOL_SIZE.getName());
    }

    /**
     * Sets the minimum pool size for pooled connections.
     *
     * @param poolSize The minimum pool size for pooled connections.
     */
    public void setMinPoolSize(final String poolSize) {
        setProperty(DocumentDbConnectionProperty.MIN_POOL_SIZE.getName(), poolSize);
    }

    /**
     * Gets the maximum wait time a thread may wait for a connection to become available.
     *
     * @return The wait queue timeout in milliseconds.
     */
    public Long getWaitQueueTimeout() {
        return getPropertyAsLong(DocumentDbConnectionProperty.WAIT_QUEUE_TIMEOUT_MS.getName());
    }

    /**
     * Sets the maximum wait time a thread may wait for a connection to become available.
     *
     * @param timeout The wait queue timeout in milliseconds.
     */
    public void setWaitQueueTimeout(final String timeout) {
        setProperty(DocumentDbConnectionProperty.WAIT_QUEUE_TIMEOUT_MS.getName(), timeout);
    }

    /**
     * Gets the maximum idle time for a pooled connection.
     *
     * @return The maximum idle time in milliseconds.
     */
    public Integer getMaxIdleTime() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.MAX_IDLE_TIME_MS.getName());
    }

    /**
     * Sets the maximum idle time for a pooled connection.
     *
     * @param idleTime The maximum idle time in milliseconds.
     */
    public void setMaxIdleTime(final String idleTime) {
        setProperty(DocumentDbConnectionProperty.MAX_IDLE_TIME_MS.getName(), idleTime);
    }

    /**
     * Gets the maximum life time for a pooled connection
     *
     * @return The maximum life time for a pooled connection in milliseconds.
     */
    public Integer getMaxLifeTime() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.MAX_LIFE_TIME_MS.getName());
    }

    /**
     * Sets the maximum life time for a pooled connection
     *
     * @param lifeTime The maximum life time for a pooled connection in milliseconds.
     */
    public void setMaxLifeTime(final String lifeTime) {
        setProperty(DocumentDbConnectionProperty.MAX_LIFE_TIME_MS.getName(), lifeTime);
    }

    /**
     * Gets the read preference when connecting as a replica set.
     *
     * @return The read preference as a ReadPreference object.
     */
    public ReadPreference getReadPreference() {
        return getPropertyAsReadPreference(DocumentDbConnectionProperty.READ_PREFERENCE.getName());
    }

    /**
     * Sets the read preference when connecting as a replica set.
     *
     * @param readPreference The name of the read preference.
     */
    public void setReadPreference(final String readPreference) {
        setProperty(DocumentDbConnectionProperty.READ_PREFERENCE.getName(), readPreference);
    }

    /**
     * Gets the UUID representation to use when encoding instances of UUID and when decoding BSON
     * binary values with subtype of 3.
     *
     * @return The UUID representation to use as a UUIDRepresentation object.
     */
    public UuidRepresentation getUUIDRepresentation() {
        return getPropertyAsUuidRepresentation(
                DocumentDbConnectionProperty.UUID_REPRESENTATION.getName());
    }

    /**
     * Sets the UUID representation to use when encoding instances of UUID and when decoding BSON
     * binary values with subtype of 3.
     *
     * @param uuidRepresentation The name of the UUID representation.
     */
    public void setUUIDRepresentation(final String uuidRepresentation) {
        setProperty(DocumentDbConnectionProperty.UUID_REPRESENTATION.getName(), uuidRepresentation);
    }


    /**
     * Attempts to retrieve a property as a UUIDRepresentation.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as a UUIDRepresentation or null if it did not exists or was
     * not a valid UUIDRepresentation.
     */
    private UuidRepresentation getPropertyAsUuidRepresentation(@NonNull final String key) {
        UuidRepresentation property = null;
        try {
            if (getProperty(key) != null) {
                property = UuidRepresentation.valueOf(getProperty(key));
            }
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Property {{}} was ignored as it was not a valid UUID representation", key, e);
        }
        return property;
    }

    /**
     * Attempts to retrieve a property as a ReadPreference.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as a ReadPreference or null if it did not exist or was not a
     * valid ReadPreference.
     */
    private ReadPreference getPropertyAsReadPreference(@NonNull final String key) {
        ReadPreference property = null;
        try {
            if (getProperty(key) != null) {
                property = ReadPreference.valueOf(getProperty(key));
            }
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Property {{}} was ignored as it was not a valid read preference.", key, e);
        }
        return property;
    }

    /**
     * Attempts to retrieve a property as a Long.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as a Long or null if it did not exist or could not be parsed.
     */
    private Long getPropertyAsLong(@NonNull final String key) {
        Long property = null;
        try {
            if (getProperty(key) != null) {
                property = Long.parseLong(getProperty(key));
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Property {{}} was ignored as it was not of type long.", key, e);
        }
        return property;
    }

    /**
     * Attempts to retrieve a property as an Integer.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as an Integer or null if it did not exist or could not be
     * parsed.
     */
    private Integer getPropertyAsInteger(@NonNull final String key) {
        Integer property = null;
        try {
            if (getProperty(key) != null) {
                property = Integer.parseInt(getProperty(key));
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Property {{}} was ignored as it was not of type integer.",  key, e);
        }
        return property;
    }

}
