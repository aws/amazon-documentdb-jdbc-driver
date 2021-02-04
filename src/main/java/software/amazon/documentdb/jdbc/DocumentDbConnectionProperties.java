package software.amazon.documentdb.jdbc;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

public class DocumentDbConnectionProperties extends Properties {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnectionProperties.class.getName());

    /**
     * Constructor for DocumentDbConnectionProperties, initializes with given properties.
     *
     * @param properties Properties to initialize with.
     */
    public DocumentDbConnectionProperties(final Properties properties) {
        // Copy properties.
        this.putAll(properties);
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
     * Get the timeout for opening a connection.
     *
     * @return The connect timeout in seconds.
     */
    public Integer getLoginTimeout() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC.getName());
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @param timeout The connect timeout in seconds.
     */
    public void setLoginTimeout(final String timeout) {
        setProperty(DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC.getName(), timeout);
    }

    /**
     * Gets the read preference when connecting as a replica set.
     *
     * @return The read preference as a ReadPreference object.
     */
    public DocumentDbReadPreference getReadPreference() {
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
     * Attempts to retrieve a property as a ReadPreference.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as a ReadPreference or null if it did not exist or was not a
     * valid ReadPreference.
     */
    private DocumentDbReadPreference getPropertyAsReadPreference(@NonNull final String key) {
        DocumentDbReadPreference property = null;
        try {
            if (getProperty(key) != null) {
                property = DocumentDbReadPreference.fromString(getProperty(key));
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

    /**
     * Validates the existing properties.
     * @throws SQLException if the required properties are not correctly set.
     */
    public void validateRequiredProperties() throws SQLException {
        if (isNullOrWhitespace(getUser())
                || isNullOrWhitespace(getPassword())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_USER_PASSWORD
            );
        }
        if (isNullOrWhitespace(getDatabase())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_DATABASE
            );
        }
        if (isNullOrWhitespace(getHostname())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_HOSTNAME
            );
        }
    }

    protected static boolean isNullOrWhitespace(@Nullable final String value) {
        return value == null || Pattern.matches("^\\s*$", value);
    }

}
