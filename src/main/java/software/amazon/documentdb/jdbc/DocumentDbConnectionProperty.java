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

import software.amazon.documentdb.jdbc.common.utilities.ConnectionProperty;

import java.util.Arrays;

/**
 * The enumeration of connection properties.
 */
public enum DocumentDbConnectionProperty implements ConnectionProperty {
    USER("user", "",
            "User name used for SCRAM-based authentication."),
    PASSWORD("password","",
            "Password used for SCRAM-based authentication"),
    HOSTNAME("host", "",
            "The host name or IP address of the DocumentDB server or cluster."),
    DATABASE("database", "",
            "The name of the database to connect to in DocumentDB."),
    READ_PREFERENCE("readPreference", "primary",
            "The read preference"),
    APPLICATION_NAME(ConnectionProperty.APPLICATION_NAME, DocumentDbConnectionProperties.DEFAULT_APPLICATION_NAME,
            "Sets the logical name of the application. The application name may be used by the client to identify the application to the server, for use in server logs, slow query logs, and profile collection."),
    REPLICA_SET("replicaSet", "rs0",
            "Implies that the hosts given are a seed list, and the driver will attempt to find all members of the set."),
    TLS_ENABLED("tls", "true",
            "Whether to connect using TLS"),
    TLS_ALLOW_INVALID_HOSTNAMES("tlsAllowInvalidHostnames", "false",
            "Whether to allow invalid host names for TLS connections. Equivalent to tlsInsecure."),
    LOGIN_TIMEOUT_SEC("loginTimeoutSec", "10",
            "How long a connection can take to be opened before timing out (in seconds)."),
    RETRY_READS_ENABLED("retryReads", "true",
            "If true the driver will retry supported read operations if they fail due to a network error. Defaults to true."),
    METADATA_SCAN_METHOD("scanMethod", "random",
            "Method of scanning for metadata."),
    METADATA_SCAN_LIMIT("scanLimit", "1000",
            "Number of records to scan for metadata"),
    TLS_CA_FILE("tlsCAFile", "",
            "The path to the Certificate Authority (CA) '.pem' file."),
    SCHEMA_NAME("schemaName", "_default",
            "The name of the stored schema to use."),
    SSH_USER("sshUser", "",
            "The user name for the SSH tunnel."),
    SSH_HOSTNAME("sshHost", "",
            "The host name for the SSH tunnel. Optionally the SSH tunnel port number can be "
                    + "provided using the syntax '<ssh-host>:<port>'. The default port is '22'."),
    SSH_PRIVATE_KEY_FILE("sshPrivateKeyFile", "",
            "The path to the private key file for the SSH tunnel."),
    SSH_PRIVATE_KEY_PASSPHRASE("sshPrivateKeyPassphrase", "",
            "If the SSH tunnel private key file is passphrase protected, "
                    + "provide the passphrase using this option."),
    SSH_STRICT_HOST_KEY_CHECKING("sshStrictHostKeyChecking", "true",
            "If true, the 'known_hosts' file is checked to ensure the target host is trusted when creating the SSH tunnel. If false, the target host is not checked. Default is 'false'."),
    SSH_KNOWN_HOSTS_FILE("sshKnownHostsFile", "",
            "The path to the 'known_hosts' file used for checking the target host for the SSH tunnel when option 'sshStrictHostKeyChecking' is 'true'. Default is '~/.ssh/known_hosts'."),
    DEFAULT_FETCH_SIZE("defaultFetchSize", String.valueOf(DocumentDbConnectionProperties.FETCH_SIZE_DEFAULT),
            "The default fetch size (in records) when retrieving results from Amazon DocumentDB. It is the number of records to retrieve in a single batch. The maximum number of records retrieved in a single batch may also be limited by the overall memory size of the result. The value can be changed by calling the `Statement.setFetchSize` JDBC method. Default is '2000'."),
    REFRESH_SCHEMA("refreshSchema", "false",
            "Refreshes any existing schema with a newly generated schema when the connection first requires the schema. Note that this will remove any existing schema customizations and will reduce performance for the first query or metadata inquiry."),
    DEFAULT_AUTH_DB("defaultAuthDb", "admin", "The default authentication database to use."),
    ;

    // Unsupported MongoDB connection properties that will be ignored but should have warnings.
    private static final String[] UNSUPPORTED_MONGO_DB_PROPERTIES = {
            "authMechanism",
            "authMechanismProperties",
            "authSource",
            "gssapiServiceName",
            "serverSelectionTimeoutMS",
            "serverSelectionTryOnce",
            "localThresholdMS",
            "heartbeatFrequencyMS",
            "ssl",
            "sslInvalidHostnamesAllowed",
            "sslAllowInvalidCertificates",
            "sslPEMKeyFile",
            "sslPEMKeyPassword",
            "sslCAFile",
            "tlsInsecure",
            "tlsCertificateKeyFile",
            "tlsCertificateKeyFilePassword",
            "tlsAllowInvalidCertificates",
            "connectTimeoutMS",
            "socketTimeoutMS",
            "maxIdleTimeMS",
            "maxLifeTimeMS",
            "maxPoolSize",
            "minPoolSize",
            "waitQueueMultiple",
            "waitQueueTimeoutMS",
            "safe",
            "journal",
            "w",
            "retryWrites",
            "wtimeoutMS",
            "readPreferenceTags",
            "readConcernLevel",
            "maxStalenessSeconds",
            "compressors",
            "zlibCompressionLevel",
            "uuidRepresentation"
    };

    private final String connectionProperty;
    private final String defaultValue;
    private final String description;

    /**
     * DocumentDbConnectionProperty constructor.
     *
     * @param connectionProperty String representing the connection property.
     * @param defaultValue String representing the default value of the property.
     * @param description Description of the property.
     */
    DocumentDbConnectionProperty(
            final String connectionProperty,
            final String defaultValue,
            final String description) {
        this.connectionProperty = connectionProperty;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    /**
     * Gets connection property.
     *
     * @return the connection property.
     */
    public String getName() {
        return connectionProperty;
    }

    /**
     * Gets the default value of the connection property.
     *
     * @return the default value of the connection property.
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Gets description.
     *
     * @return the description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public static boolean isSupportedProperty(final String name) {
        return Arrays
                .stream(DocumentDbConnectionProperty.values())
                .anyMatch(value -> value.getName().equals(name));
    }

    /**
     * Check if the property is unsupported by the driver but still a valid MongoDB option.
     *
     * @param name The name of the property.
     * @return {@code true} if property is valid but unsupported; {@code false} otherwise.
     */
    public static boolean isUnsupportedMongoDBProperty(final String name) {
        return Arrays.asList(UNSUPPORTED_MONGO_DB_PROPERTIES).contains(name);
    }

    static DocumentDbConnectionProperty getPropertyFromKey(final String key) {
        for (DocumentDbConnectionProperty connectionProperty: DocumentDbConnectionProperty.values()) {
            if (connectionProperty.getName().equals(key)) {
                return connectionProperty;
            }
        }
        return null;
    }
}
