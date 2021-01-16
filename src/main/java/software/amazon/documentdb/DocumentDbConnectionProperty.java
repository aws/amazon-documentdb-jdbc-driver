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

import software.amazon.jdbc.utilities.ConnectionProperty;
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
    SERVER_SELECTION_TIMEOUT_MS("serverSelectionTimeoutMS", "",
            "How long the driver will wait for server selection to succeed before throwing an exception."),
    LOCAL_THRESHOLD_MS("localThresholdMS", "",
            "When choosing among multiple MongoDB servers to send a request, the driver will only send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the local threshold."),
    HEARTBEAT_FREQUENCY_MS("heartbeatFrequencyMS", "",
            "The frequency that the driver will attempt to determine the current state of each server in the cluster."),
    TLS_ENABLED("tls", "false",
            "Whether to connect using TLS"),
    TLS_ALLOW_INVALID_HOSTNAMES("tlsAllowInvalidHostnames", "false",
            "Whether to allow invalid host names for TLS connections."),
    CONNECT_TIMEOUT_MS("connectTimeoutMS", "",
            "How long a connection can take to be opened before timing out (in milliseconds)."),
    SOCKET_TIMEOUT_MS("socketTimeoutMS", "",
            "How long a send or receive on a socket can take before timing out."),
    MAX_POOL_SIZE("maxPoolSize", "",
            "The maximum number of connections in the connection pool."),
    MIN_POOL_SIZE("minPoolSize", "",
            "The minimum number of connections in the connection pool."),
    WAIT_QUEUE_TIMEOUT_MS("waitQueueTimeoutMS", "",
            "The maximum wait time in milliseconds that a thread may wait for a connection to become available."),
    MAX_IDLE_TIME_MS("maxIdleTimeMS", "",
            "Maximum idle time of a pooled connection (in milliseconds)."),
    MAX_LIFE_TIME_MS("maxLifeTimeMS", "",
            " Maximum life time of a pooled connection (in milliseconds)."),
    RETRY_READS_ENABLED("retryReads", "true",
            "If true the driver will retry supported read operations if they fail due to a network error. Defaults to true."),
    UUID_REPRESENTATION("uuidRepresentation", "",
            "The UUID representation to use when encoding instances of UUID and when decoding BSON binary values with subtype of 3. Available values: unspecified|standard|javaLegacy|csharpLegacy|pythonLegacy.");

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
    public String getConnectionProperty() {
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
                .anyMatch(value -> value.getConnectionProperty().equals(name));
    }
}
