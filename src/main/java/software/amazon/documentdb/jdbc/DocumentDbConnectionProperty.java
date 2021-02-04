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
            "Whether to allow invalid host names for TLS connections."),
    LOGIN_TIMEOUT_SEC("loginTimeoutSec", "",
            "How long a connection can take to be opened before timing out (in seconds)."),
    RETRY_READS_ENABLED("retryReads", "true",
            "If true the driver will retry supported read operations if they fail due to a network error. Defaults to true.");

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
}
