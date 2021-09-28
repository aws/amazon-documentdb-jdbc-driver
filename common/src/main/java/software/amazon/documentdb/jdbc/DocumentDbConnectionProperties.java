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

import com.google.common.base.Strings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.ServerMonitorListener;
import lombok.SneakyThrows;
import nl.altindag.ssl.SSLFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentDbConnectionProperties extends Properties {

    public static final String DOCUMENT_DB_SCHEME = "jdbc:documentdb:";
    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String USER_HOME_PATH_NAME = System.getProperty(USER_HOME_PROPERTY);
    public static final String DOCUMENTDB_HOME_PATH_NAME = Paths.get(
            USER_HOME_PATH_NAME, ".documentdb").toString();
    public static final String CLASS_PATH_LOCATION_NAME = getClassPathLocation();
    static final String[] SSH_PRIVATE_KEY_FILE_SEARCH_PATHS = {
                USER_HOME_PATH_NAME,
                DOCUMENTDB_HOME_PATH_NAME,
                CLASS_PATH_LOCATION_NAME,
        };

    private static final String AUTHENTICATION_DATABASE = "admin";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnectionProperties.class.getName());
    private static final Pattern WHITE_SPACE_PATTERN = Pattern.compile("^\\s*$");
    private static final String ROOT_PEM_RESOURCE_FILE_NAME = "/rds-ca-2019-root.pem";
    public static final String HOME_PATH_PREFIX_REG_EXPR = "^~[/\\\\].*$";
    public static final int FETCH_SIZE_DEFAULT = 2000;

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

    private static String getClassPathLocation() {
        String classPathLocation = null;
        try {
            final Path classParentPath = Paths.get(DocumentDbConnectionProperties.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI()).getParent();
            if (classParentPath != null) {
                classPathLocation = classParentPath.toString();
            }
        } catch (URISyntaxException e) {
            // Ignore error, return null.
        }
        return classPathLocation;
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
     * @return tlsEnabled {@code true} if TLS/SSL is enabled; {@code false} otherwise.
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
     * Gets the method of scanning for metadata.
     *
     * @return The method of scanning for metadata.
     */
    public DocumentDbMetadataScanMethod getMetadataScanMethod() {
        return getPropertyAsScanMethod(DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getName());
    }

    /**
     * Sets the method of scanning for metadata.
     *
     * @param method The name of the scan method.
     */
    public void setMetadataScanMethod(final String method) {
        setProperty(DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getName(), method);
    }

    /**
     * Gets the number of records to scan while determining schema.
     *
     * @return Integer representing the number of records to scan.
     */
    public int getMetadataScanLimit() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getName());
    }

    /**
     * Sets the number of records to scan while determining schema.
     *
     * @param limit The name of the read preference.
     */
    public void setMetadataScanLimit(final String limit) {
        setProperty(DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getName(), limit);
    }

    /**
     * Gets the schema persisted storage type.
     *
     * @return the schema persisted storage type.
     */
    public DocumentDbSchemaStoreType getPersistedSchemaStore() {
        return getPropertyAsSchemaStoreType(
                DocumentDbConnectionProperty.SCHEMA_PERSISTENCE_STORE.getName());
    }

    /**
     * Sets the schema persisted storage type.
     *
     * @param persistedStore the schema persisted storage type.
     */
    public void setPersistedSchemaStore(final DocumentDbSchemaStoreType persistedStore) {
        setProperty(
                DocumentDbConnectionProperty.SCHEMA_PERSISTENCE_STORE.getName(),
                persistedStore.getName());
    }

    /**
     * Gets the schema name for persisted schema.
     *
     * @return the name of the schema.
     */
    public String getSchemaName() {
        return getProperty(DocumentDbConnectionProperty.SCHEMA_NAME.getName(),
                DocumentDbConnectionProperty.SCHEMA_NAME.getDefaultValue());
    }

    /**
     * Sets the schema name for persisted schema.
     *
     * @param schemaName the name of the schema.
     */
    public void setSchemaName(final String schemaName) {
        setProperty(DocumentDbConnectionProperty.SCHEMA_NAME.getName(), schemaName);
    }

    /**
     * Sets the TLS CA file path.
     *
     * @param tlsCAFilePath the TLS CA file path.
     */
    public void setTlsCAFilePath(final String tlsCAFilePath) {
        setProperty(DocumentDbConnectionProperty.TLS_CA_FILE.getName(), tlsCAFilePath);
    }

    /**
     * Gets the TLS CA file path.
     *
     * @return a String representing the TLS CA file path, if set, null otherwise.
     */
    public String getTlsCAFilePath() {
        return getProperty(DocumentDbConnectionProperty.TLS_CA_FILE.getName());
    }

    /**
     * Sets the SSH tunnel user.
     *
     * @param sshUser the SSH tunnel user.
     */
    public void setSshUser(final String sshUser) {
        setProperty(DocumentDbConnectionProperty.SSH_USER.getName(), sshUser);
    }

    /**
     * Gets the SSH tunnel user.
     *
     * @return the SSH tunnel user.
     */
    public String getSshUser() {
        return getProperty(DocumentDbConnectionProperty.SSH_USER.getName());
    }

    /**
     * Sets the SSH tunnel host name. Can optionally contain the port number using `host-name:port'
     * syntax. If port is not provided, port 22 is assumed.
     *
     * @param sshHostname the SSH tunnel host name and optional port number.
     */
    public void setSshHostname(final String sshHostname) {
        setProperty(DocumentDbConnectionProperty.SSH_HOSTNAME.getName(), sshHostname);
    }

    /**
     * Gets the SSH tunnel host name and optional port number.
     *
     * @return the SSH tunnel host name and optional port number.
     */
    public String getSshHostname() {
        return getProperty(DocumentDbConnectionProperty.SSH_HOSTNAME.getName());
    }

    /**
     * Sets the file path of the private key file. Can be prefixed with '~' to indicate the
     * current user's home directory.
     *
     * @param sshPrivateKeyFile the file path of the private key file.
     */
    public void setSshPrivateKeyFile(final String sshPrivateKeyFile) {
        setProperty(DocumentDbConnectionProperty.SSH_PRIVATE_KEY_FILE.getName(), sshPrivateKeyFile);
    }

    /**
     * Gets the file path of the private key file.
     *
     * @return the file path of the private key file.
     */
    public String getSshPrivateKeyFile() {
        return getProperty(DocumentDbConnectionProperty.SSH_PRIVATE_KEY_FILE.getName());
    }

    /**
     * Sets the passphrase of the private key file. If not set, no passphrase will be used.
     *
     * @param sshPrivateKeyPassphrase the passphrase of the private key file
     */
    public void setSshPrivateKeyPassphrase(final String sshPrivateKeyPassphrase) {
        setProperty(
                DocumentDbConnectionProperty.SSH_PRIVATE_KEY_PASSPHRASE.getName(),
                sshPrivateKeyPassphrase);
    }

    /**
     * Gets the passphrase of the private key file.
     *
     * @return the passphrase of the private key file
     */
    public String getSshPrivateKeyPassphrase() {
        return getProperty(DocumentDbConnectionProperty.SSH_PRIVATE_KEY_PASSPHRASE.getName());
    }

    /**
     * Sets the indicator for whether the SSH tunnel will perform strict host key checking. When
     * {@code true}, the 'known_hosts' file is checked to ensure the hashed host key is the same
     * as the target host.
     *
     * @param sshStrictHostKeyChecking the indicator for whether the SSH tunnel will perform strict
     *                                 host key checking.
     */
    public void setSshStrictHostKeyChecking(final String sshStrictHostKeyChecking) {
        setProperty(
                DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getName(),
                String.valueOf(Boolean.parseBoolean(sshStrictHostKeyChecking)));
    }

    /**
     * Gets the indicator for whether the SSH tunnel will perform strict host key checking.
     *
     * @return the indicator for whether the SSH tunnel will perform strict host key checking.
     */
    public boolean getSshStrictHostKeyChecking() {
        return Boolean.parseBoolean(getProperty(
                DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getName(),
                DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getDefaultValue()));
    }

    /**
     * Gets the file path to the 'known_hosts' file. If not set, '~/.ssh/known_hosts' is assumed.
     *
     * @param sshKnownHostsFile the file path to the 'known_hosts' file.
     */
    public void setSshKnownHostsFile(final String sshKnownHostsFile) {
        setProperty(DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE.getName(), sshKnownHostsFile);
    }

    /**
     * Gets the file path to the 'known_hosts' file.
     *
     * @return the file path to the 'known_hosts' file.
     */
    public String getSshKnownHostsFile() {
        return getProperty(DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE.getName());
    }

    /**
     * Sets the default fetch size (in records) when retrieving results from Amazon DocumentDB.
     * It is the number of records to retrieve in a single batch.
     * The maximum number of records retrieved in a single batch may also be limited by the overall
     * memory size of the result. The value can be changed by calling the `Statement.setFetchSize`
     * JDBC method. Default is '2000'.
     *
     * @param defaultFetchSize the default fetch size (in records) when retrieving results from Amazon DocumentDB.
     */
    public void setDefaultFetchSize(final String defaultFetchSize) {
        setProperty(DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE.getName(), defaultFetchSize);
    }

    /**
     * Gets the default fetch size (in records) when retrieving results from Amazon DocumentDB.
     * It is the number of records to retrieve in a single batch.
     * The maximum number of records retrieved in a single batch may also be limited by the overall
     * memory size of the result. The value can be changed by calling the `Statement.setFetchSize`
     * JDBC method. Default is '2000'.
     *
     * @return the default fetch size (in records) when retrieving results from Amazon DocumentDB.
     */
    public Integer getDefaultFetchSize() {
        return getPropertyAsInteger(DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE.getName());
    }

    /**
     * Sets indicator of whether to refresh any existing schema with a newly generated schema when
     * the connection first requires the schema. Note that this will remove any existing schema
     * customizations and will reduce performance for the first query or metadata inquiry.
     *
     * @param refreshSchema  indicator of whether to refresh any existing schema with a newly
     *                       generated schema when the connection first requires the schema.
     *                       Note that this will remove any existing schema customizations and
     *                       will reduce performance for the first query or metadata inquiry.
     */
    public void setRefreshSchema(final String refreshSchema) {
        setProperty(DocumentDbConnectionProperty.REFRESH_SCHEMA.getName(), refreshSchema);
    }

    /**
     * Gets indicator of whether to refresh any existing schema with a newly generated schema when
     * the connection first requires the schema. Note that this will remove any existing schema
     * customizations and will reduce performance for the first query or metadata inquiry.
     *
     * @return indicator of whether to refresh any existing schema with a newly generated schema
     *         when the connection first requires the schema. Note that this will remove any
     *         existing schema customizations and will reduce performance for the first query or
     *         metadata inquiry.
     */
    public Boolean getRefreshSchema() {
        return Boolean.parseBoolean(getProperty(
                        DocumentDbConnectionProperty.REFRESH_SCHEMA.getName(),
                        DocumentDbConnectionProperty.REFRESH_SCHEMA.getDefaultValue()));
    }

    /**
     * Builds the MongoClientSettings from properties.
     *
     * @return a {@link MongoClientSettings} object.
     */
    public MongoClientSettings buildMongoClientSettings() {
        return buildMongoClientSettings(null);
    }

    /**
     * Builds the MongoClientSettings from properties.
     *
     * @param sshLocalPort the local port number for an internal SSH tunnel. A port number of zero
     *                     indicates there is no valid internal SSH tunnel started.
     * @return a {@link MongoClientSettings} object.
     */
    public MongoClientSettings buildMongoClientSettings(final int sshLocalPort) {
        return buildMongoClientSettings(null, sshLocalPort);
    }

    /**
     * Builds the MongoClientSettings from properties.
     *
     * @param serverMonitorListener the server monitor listener
     * @return a {@link MongoClientSettings} object.
     */
    public MongoClientSettings buildMongoClientSettings(
            final ServerMonitorListener serverMonitorListener) {
        return buildMongoClientSettings(serverMonitorListener, 0);
    }

    /**
     * Builds the MongoClientSettings from properties.
     *
     * @param serverMonitorListener the server monitor listener
     * @param sshLocalPort the local port number for an internal SSH tunnel. A port number of zero
     *                     indicates there is no valid internal SSH tunnel started.
     * @return a {@link MongoClientSettings} object.
     */
    public MongoClientSettings buildMongoClientSettings(
            final ServerMonitorListener serverMonitorListener,
            final int sshLocalPort) {

        final MongoClientSettings.Builder clientSettingsBuilder = MongoClientSettings.builder();

        // Create credential for admin database (only authentication database in DocumentDB).
        final String user = getUser();
        final String password = getPassword();
        if (user != null && password != null) {
            final MongoCredential credential =
                    MongoCredential.createCredential(user, AUTHENTICATION_DATABASE, password.toCharArray());
            clientSettingsBuilder.credential(credential);
        }

        // Set the server configuration.
        applyServerSettings(clientSettingsBuilder, serverMonitorListener);

        // Set the cluster configuration.
        applyClusterSettings(clientSettingsBuilder, sshLocalPort);

        // Set the socket configuration.
        applySocketSettings(clientSettingsBuilder);

        // Set the SSL/TLS configuration.
        applyTlsSettings(clientSettingsBuilder);

        // Set the read preference.
        final DocumentDbReadPreference readPreference = getReadPreference();
        if (readPreference != null) {
            clientSettingsBuilder.readPreference(ReadPreference.valueOf(
                    readPreference.getName()));
        }

        // Get retry reads.
        final boolean retryReads = getRetryReadsEnabled();
        clientSettingsBuilder
                .applicationName(getApplicationName())
                .retryReads(retryReads)
                // NOTE: DocumentDB does not support retryWrites option. (2020-05-13)
                // https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html#functional-differences.retryable-writes
                .retryWrites(false)
                .build();

        return clientSettingsBuilder.build();
    }

    /**
     * Builds the sanitized connection string from properties.
     *
     * @return a {@link String} with the sanitized connection properties.
     */
    public String buildSanitizedConnectionString() {
        final String connectionStringTemplate = "//%s%s/%s%s";
        final String loginInfo = isNullOrWhitespace(getUser()) ? "" : getUser() + "@";
        final String hostInfo = isNullOrWhitespace(getHostname()) ? "" : getHostname();
        final String databaseInfo = isNullOrWhitespace(getDatabase()) ? "" : getDatabase();
        final StringBuilder optionalInfo = new StringBuilder();
        if (getApplicationName() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.APPLICATION_NAME, getApplicationName());
        }
        if (getLoginTimeout() != Integer.parseInt(DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC, getLoginTimeout());
        }
        if (getMetadataScanLimit() != Integer.parseInt(DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.METADATA_SCAN_LIMIT, getMetadataScanLimit());
        }
        if (getMetadataScanMethod() != DocumentDbMetadataScanMethod.fromString(DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.METADATA_SCAN_METHOD, getMetadataScanMethod().getName());
        }
        if (getRetryReadsEnabled() != Boolean.parseBoolean(DocumentDbConnectionProperty.RETRY_READS_ENABLED.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.RETRY_READS_ENABLED, getRetryReadsEnabled());
        }
        if (getReadPreference() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.READ_PREFERENCE, getReadPreference().getName());
        }
        if (getReplicaSet() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.REPLICA_SET, getReplicaSet());
        }
        if (getTlsEnabled() != Boolean.parseBoolean(DocumentDbConnectionProperty.TLS_ENABLED.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.TLS_ENABLED, getTlsEnabled());
        }
        if (getTlsAllowInvalidHostnames() != Boolean.parseBoolean(DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES, getTlsAllowInvalidHostnames());
        }
        if (getTlsCAFilePath() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.TLS_CA_FILE, getTlsCAFilePath());
        }
        if (!DocumentDbConnectionProperty.SCHEMA_NAME.getDefaultValue().equals(getSchemaName())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SCHEMA_NAME, getSchemaName());
        }
        if (getSshUser() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SSH_USER, getSshUser());
        }
        if (getSshHostname() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SSH_HOSTNAME, getSshHostname());
        }
        if (getSshPrivateKeyFile() != null) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SSH_PRIVATE_KEY_FILE, getSshPrivateKeyFile());
        }
        if (getSshStrictHostKeyChecking() != Boolean.parseBoolean(DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING, getSshStrictHostKeyChecking());
        }
        if (getSshKnownHostsFile() != null && !DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE.getDefaultValue().equals(getSshKnownHostsFile())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE, getSshKnownHostsFile());
        }
        if (getDefaultFetchSize() != Integer.parseInt(DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE, getDefaultFetchSize());
        }
        if (getRefreshSchema() != Boolean.parseBoolean(DocumentDbConnectionProperty.REFRESH_SCHEMA.getDefaultValue())) {
            appendOption(optionalInfo, DocumentDbConnectionProperty.REFRESH_SCHEMA, getRefreshSchema());
        }
        return String.format(connectionStringTemplate,
                loginInfo,
                hostInfo,
                databaseInfo,
                optionalInfo);
    }

    private void appendOption(final StringBuilder optionInfo,
            final DocumentDbConnectionProperty option,
            final Object optionValue) {
        optionInfo.append(optionInfo.length() == 0 ? "?" : "&");
        optionInfo.append(option.getName()).append("=").append(optionValue);
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
                    SqlState.INVALID_PARAMETER_VALUE,
                    SqlError.MISSING_USER_PASSWORD
            );
        }
        if (isNullOrWhitespace(getDatabase())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_PARAMETER_VALUE,
                    SqlError.MISSING_DATABASE
            );
        }
        if (isNullOrWhitespace(getHostname())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_PARAMETER_VALUE,
                    SqlError.MISSING_HOSTNAME
            );
        }
    }

    /**
     * Gets the connection properties from the connection string.
     *
     * @param documentDbUrl the given properties.
     * @return a {@link DocumentDbConnectionProperties} with the properties set.
     * @throws SQLException if connection string is invalid.
     */
    public static DocumentDbConnectionProperties getPropertiesFromConnectionString(final String documentDbUrl)
            throws SQLException {
        return getPropertiesFromConnectionString(new Properties(), documentDbUrl, DOCUMENT_DB_SCHEME);
    }


    /**
     * Gets the connection properties from the connection string.
     *
     * @param info the given properties.
     * @param documentDbUrl the connection string.
     * @return a {@link DocumentDbConnectionProperties} with the properties set.
     * @throws SQLException if connection string is invalid.
     */
    public static DocumentDbConnectionProperties getPropertiesFromConnectionString(
            final Properties info, final String documentDbUrl, final String connectionStringPrefix)
            throws SQLException {

        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(info);
        final String postSchemeSuffix = documentDbUrl.substring(connectionStringPrefix.length());
        if (!isNullOrWhitespace(postSchemeSuffix)) {
            try {
                final URI uri = new URI(postSchemeSuffix);

                setHostName(properties, uri);

                setUserPassword(properties, uri);

                setDatabase(properties, uri);

                setOptionalProperties(properties, uri);

            } catch (URISyntaxException e) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.INVALID_CONNECTION_PROPERTIES,
                        documentDbUrl
                );
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }

        properties.validateRequiredProperties();
        return properties;
    }

    private static void setDatabase(final Properties properties, final URI mongoUri)
            throws SQLException {
        if (isNullOrWhitespace(mongoUri.getPath())) {
            if (properties.getProperty(
                    DocumentDbConnectionProperty.DATABASE.getName(), null) == null) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.MISSING_DATABASE);
            }
            return;
        }

        final String database = mongoUri.getPath().substring(1);
        addPropertyIfNotSet(properties, DocumentDbConnectionProperty.DATABASE.getName(), database);

    }

    private static void setOptionalProperties(final Properties properties, final URI mongoUri)
            throws UnsupportedEncodingException {
        final String query = mongoUri.getQuery();
        if (isNullOrWhitespace(query)) {
            return;
        }
        final String[] propertyPairs = query.split("&");
        for (String pair : propertyPairs) {
            final int splitIndex = pair.indexOf("=");
            final String key = pair.substring(0, splitIndex);
            final String value = pair.substring(1 + splitIndex);

            addPropertyIfValid(properties, key, value);
        }
    }

    private static void setUserPassword(final Properties properties, final URI mongoUri)
            throws UnsupportedEncodingException, SQLException {
        if (mongoUri.getUserInfo() == null) {
            if (properties.getProperty(
                    DocumentDbConnectionProperty.USER.getName(), null) == null
                    || properties.getProperty(
                    DocumentDbConnectionProperty.PASSWORD.getName(), null) == null) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.MISSING_USER_PASSWORD);
            }
            return;
        }

        final String userPassword = mongoUri.getUserInfo();

        // Password is optional
        final int userPasswordSeparatorIndex = userPassword.indexOf(":");
        if (userPasswordSeparatorIndex >= 0) {
            addPropertyIfNotSet(properties, DocumentDbConnectionProperty.USER.getName(),
                    userPassword.substring(0, userPasswordSeparatorIndex));
            addPropertyIfNotSet(properties, DocumentDbConnectionProperty.PASSWORD.getName(),
                    userPassword.substring(userPasswordSeparatorIndex + 1));
        } else {
            addPropertyIfNotSet(properties, DocumentDbConnectionProperty.USER.getName(),
                    userPassword);
        }
    }

    private static void setHostName(final Properties properties, final URI mongoUri) throws SQLException {
        String hostName = mongoUri.getHost();
        if (hostName == null) {
            if (properties.getProperty(
                    DocumentDbConnectionProperty.HOSTNAME.getName(), null) == null) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.MISSING_HOSTNAME);
            }
            return;
        }

        if (mongoUri.getPort() > 0) {
            hostName += ":" + mongoUri.getPort();
        }
        addPropertyIfNotSet(properties, DocumentDbConnectionProperty.HOSTNAME.getName(),
                hostName);
    }

    private static void addPropertyIfValid(
            final Properties info, final String propertyKey, final String propertyValue) {
        if (DocumentDbConnectionProperty.isSupportedProperty(propertyKey)) {
            addPropertyIfNotSet(info, propertyKey, propertyValue);
        } else if (DocumentDbConnectionProperty.isUnsupportedMongoDBProperty(propertyKey)) {
            LOGGER.warn(
                    "Ignored MongoDB property: {{}} as it not supported by the driver.",
                    propertyKey);
        } else {
            LOGGER.warn("Ignored invalid property: {{}}", propertyKey);
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

    /**
     * Applies the server-related connection properties to the given client settings builder.
     *
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     * @param serverMonitorListener The server monitor listener to add as an event listener.
     */
    private void applyServerSettings(
            final MongoClientSettings.Builder clientSettingsBuilder,
            final ServerMonitorListener serverMonitorListener) {
        clientSettingsBuilder.applyToServerSettings(
                b -> {
                    if (serverMonitorListener != null) {
                        b.addServerMonitorListener(serverMonitorListener);
                    }
                });
    }

    /**
     * Applies the cluster-related connection properties to the given client settings builder.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private void applyClusterSettings(
            final MongoClientSettings.Builder clientSettingsBuilder,
            final int sshLocalPort) {
        final String host;
        if (enableSshTunnel() && isSshPrivateKeyFileExists() && sshLocalPort > 0) {
            host = String.format("localhost:%d", sshLocalPort);
        } else {
            host = getHostname();
        }
        final String replicaSetName = getReplicaSet();

        clientSettingsBuilder.applyToClusterSettings(
                b -> {
                    if (host != null) {
                        b.hosts(Collections.singletonList(new ServerAddress(host)));
                    }

                    if (replicaSetName != null) {
                        b.requiredReplicaSetName(replicaSetName);
                    }
                });
    }

    /**
     * Gets indicator of whether the options indicate the SSH port forwarding tunnel
     * should be enabled.
     *
     * @return {@code true} if the SSH port forwarding tunnel should be enabled,
     * otherwise {@code false}.
     */
    public boolean enableSshTunnel() {
        return !isNullOrWhitespace(getSshUser())
                && !isNullOrWhitespace(getSshHostname())
                && !isNullOrWhitespace(getSshPrivateKeyFile());
    }

    /**
     * Get whether the SSH private key file exists.
     *
     * @return returns {@code true} if the file exists, {@code false}, otherwise.
     */
    public boolean isSshPrivateKeyFileExists() {
        return Files.exists(getPath(getSshPrivateKeyFile(), SSH_PRIVATE_KEY_FILE_SEARCH_PATHS));
    }

    /**
     * Applies the socket-related connection properties to the given client settings builder.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private void applySocketSettings(
            final MongoClientSettings.Builder clientSettingsBuilder) {
        final Integer connectTimeout = getLoginTimeout();

        clientSettingsBuilder.applyToSocketSettings(
                b -> {
                    if (connectTimeout != null) {
                        b.connectTimeout(connectTimeout, TimeUnit.SECONDS);
                    }
                });
    }

    /**
     * Applies the TLS/SSL-related connection properties to the given client settings builder.
     * @param clientSettingsBuilder The client settings builder to apply the properties to.
     */
    private void applyTlsSettings(final MongoClientSettings.Builder clientSettingsBuilder) {
        clientSettingsBuilder.applyToSslSettings(this::applyToSslSettings);
    }

    @SneakyThrows
    private void applyToSslSettings(final SslSettings.Builder builder) {
        // Handle tls and tlsAllowInvalidHostnames options.
        final boolean tlsEnabled = getTlsEnabled();
        final boolean tlsAllowInvalidHostnames = getTlsAllowInvalidHostnames();
        builder
                .enabled(tlsEnabled)
                .invalidHostNameAllowed(tlsAllowInvalidHostnames);

        if (!tlsEnabled) {
            return;
        }

        // Handle the tlsCAFile option.
        try (InputStream inputStream = getTlsCAFileInputStream()) {
            if (inputStream != null) {
                final X509Certificate certificate = (X509Certificate) CertificateFactory
                        .getInstance("X.509")
                        .generateCertificate(inputStream);
                final SSLContext sslContext = SSLFactory.builder()
                        .withTrustMaterial(certificate)
                        .build()
                        .getSslContext();
                builder.context(sslContext);
            }
        }
    }

    private @Nullable InputStream getTlsCAFileInputStream() throws FileNotFoundException, SQLException {
        final InputStream inputStream;
        if (Strings.isNullOrEmpty(getTlsCAFilePath())) {
             inputStream = getClass().getResourceAsStream(ROOT_PEM_RESOURCE_FILE_NAME);
        } else {
            final String tlsCAFileName = getTlsCAFilePath();
            final Path tlsCAFileNamePath;
            tlsCAFileNamePath = getPath(tlsCAFileName);
            if (tlsCAFileNamePath.toFile().exists()) {
                inputStream = new FileInputStream(tlsCAFileNamePath.toFile());
            } else {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.INVALID_PARAMETER_VALUE,
                        SqlError.TLS_CA_FILE_NOT_FOUND,
                        tlsCAFileNamePath);
            }
        }
        return inputStream;
    }

    /**
     * Gets an absolute path from the given file path. It performs the substitution for a leading
     * '~' to be replaced by the user's home directory.
     *
     * @param filePath the given file path to process.
     * @return a {@link Path} for the absolution path for the given file path.
     */
    public static Path getPath(final String filePath, final String... searchFolders) {
        if (filePath.matches(HOME_PATH_PREFIX_REG_EXPR)) {
            final String fromHomePath = filePath.replaceFirst("~",
                    Matcher.quoteReplacement(USER_HOME_PATH_NAME));
            return Paths.get(fromHomePath).toAbsolutePath();
        } else {
            final Path origFilePath = Paths.get(filePath);
            if (origFilePath.isAbsolute()) {
                return origFilePath;
            }
            for (String searchFolder : searchFolders) {
                if (searchFolder == null) {
                    continue;
                }
                final Path testPath = Paths.get(searchFolder, filePath);
                if (testPath.toAbsolutePath().toFile().exists()) {
                    return testPath;
                }
            }
        }
        return Paths.get(filePath).toAbsolutePath();
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
     * Attempts to retrieve a property as a DocumentDbMetadataScanMethod.
     *
     * @param key The property to retrieve.
     * @return The retrieved property as a ReadPreference or null if it did not exist or was not a
     * valid ReadPreference.
     */
    private DocumentDbMetadataScanMethod getPropertyAsScanMethod(@NonNull final String key) {
        DocumentDbMetadataScanMethod property = null;
        try {
            if (getProperty(key) != null) {
                property = DocumentDbMetadataScanMethod.fromString(getProperty(key));
            } else if (DocumentDbConnectionProperty.getPropertyFromKey(key) != null) {
                property =  DocumentDbMetadataScanMethod.fromString(
                        DocumentDbConnectionProperty.getPropertyFromKey(key).getDefaultValue());
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
            } else if (DocumentDbConnectionProperty.getPropertyFromKey(key) != null) {
                property = Integer.parseInt(
                        DocumentDbConnectionProperty.getPropertyFromKey(key).getDefaultValue());
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Property {{}} was ignored as it was not of type integer.",  key, e);
        }
        return property;
    }

    /**
     * Attempts to retrieve a property as a DocumentDbSchemaStoreType.
     *
     * @param key the property to retrieve.
     * @return the retrieved property as a DocumentDbSchemaStoreType or null if it did not exist
     * or was not a valid ReadPreference.
     */
    private DocumentDbSchemaStoreType getPropertyAsSchemaStoreType(@NonNull final String key) {
        DocumentDbSchemaStoreType property = null;
        try {
            if (getProperty(key) != null) {
                property = DocumentDbSchemaStoreType.fromString(getProperty(key));
            } else if (DocumentDbConnectionProperty.getPropertyFromKey(key) != null) {
                property =  DocumentDbSchemaStoreType.fromString(
                        DocumentDbConnectionProperty.getPropertyFromKey(key).getDefaultValue());
            }
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Property {{}} was ignored as it was not a valid schema storage type.", key, e);
        }
        return property;
    }

    /**
     * Checks whether the value is null or contains white space.
     * @param value the value to test.
     * @return returns {@code true} if the value is null or contains white space, or {@code false},
     * otherwise.
     */
    public static boolean isNullOrWhitespace(@Nullable final String value) {
        return value == null || WHITE_SPACE_PATTERN.matcher(value).matches();
    }
}
