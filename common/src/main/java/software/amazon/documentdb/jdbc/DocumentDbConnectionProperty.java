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
    APPLICATION_NAME(ConnectionProperty.APPLICATION_NAME, "",
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
    METADATA_SCAN_METHOD("scanMethod", "natural",
            "Method of scanning for metadata."),
    METADATA_SCAN_LIMIT("scanLimit", "1000",
            "Number of records to scan for metadata");

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
            "tlsCAFile",
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

    protected static DocumentDbConnectionProperty getPropertyFromKey(final String key) {
        for (DocumentDbConnectionProperty connectionProperty: DocumentDbConnectionProperty.values()) {
            if (connectionProperty.getName().equals(key)) {
                return connectionProperty;
            }
        }
        return null;
    }
}
