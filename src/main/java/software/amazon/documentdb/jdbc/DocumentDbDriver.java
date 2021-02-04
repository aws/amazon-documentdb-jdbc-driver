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

import com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Driver;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public class DocumentDbDriver extends Driver {
    // Note: This class must be marked public for the registration/DeviceManager to work.
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbDriver.class.getName());
    private static final String DOCUMENT_DB_SCHEME = "jdbc:documentdb:";

    private static final String UTF8 = "UTF-8";

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
        final DocumentDbConnectionProperties properties;
        try {
            // Let MongoDB driver check the properties and options of the URL.
            properties = getPropertiesFromConnectionString(info, getMongoDbUrl(url));
            properties.validateRequiredProperties();
        } catch (IllegalArgumentException exception) {
            throw new SQLException(exception.getMessage(), exception);
        }

        return new DocumentDbConnection(new DocumentDbConnectionProperties(properties));
    }

    @Override
    public boolean acceptsURL(@NonNull final String url) throws SQLException {
        if (url == null) {
            throw new SQLException("The url cannot be null");
        }

        return isExpectedJdbcScheme(url);
    }

    @VisibleForTesting
    static DocumentDbConnectionProperties getPropertiesFromConnectionString(final Properties info, final String mongoDbUrl)
            throws SQLException {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(info);
        try {
            final URI mongoUri = new URI(mongoDbUrl);

            setHostName(properties, mongoUri);

            setUserPassword(properties, mongoUri);

            setDatabase(properties, mongoUri);

            setOptionalProperties(properties, mongoUri);

        } catch (URISyntaxException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.INVALID_CONNECTION_PROPERTIES
            );
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e.getMessage(), e);
        }
        return properties;
    }

    private static void setDatabase(final Properties properties, final URI mongoUri) throws UnsupportedEncodingException,
            SQLException {
        if (mongoUri.getPath() == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_DATABASE);
        }
        final String database = mongoUri.getPath().substring(1);
        addPropertyIfNotSet(properties, DocumentDbConnectionProperty.DATABASE.getName(), database);

    }

    private static void setOptionalProperties(final Properties properties, final URI mongoUri) throws UnsupportedEncodingException,
            SQLException {
        final String query = mongoUri.getQuery();
        if (isNullOrWhitespace(query)) {
            return;
        }
        final String[] propertyValues = query.split("&");
        final Map<String, String> propertyPairs = new HashMap<>();
        for (String pair: propertyValues) {
            final int splitIndex = pair.indexOf("=");
            propertyPairs.put(pair.substring(0, splitIndex),
                    pair.substring(splitIndex + 1));
        }
        for (String propertyKey: propertyPairs.keySet()) {
            checkValidProperty(propertyKey);
            addPropertyIfNotSet(properties, propertyKey, propertyPairs.get(propertyKey));
        }
    }

    private static void setUserPassword(final Properties properties, final URI mongoUri) throws UnsupportedEncodingException,
            SQLException {
        if (mongoUri.getUserInfo() == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_USER_PASSWORD);
        }
        final String userPassword = mongoUri.getUserInfo();
        addPropertyIfNotSet(properties, DocumentDbConnectionProperty.USER.getName(),
                userPassword.substring(0, userPassword.indexOf(":")));
        addPropertyIfNotSet(properties, DocumentDbConnectionProperty.PASSWORD.getName(),
                userPassword.substring(userPassword.indexOf(":") + 1));
    }

    private static void setHostName(final Properties properties, final URI mongoUri) throws SQLException {
        String hostName = mongoUri.getHost();
        if (hostName == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.MISSING_HOSTNAME);
        } else {
            if (mongoUri.getPort() > 0) {
                hostName += ":" + mongoUri.getPort();
            }
            addPropertyIfNotSet(properties, DocumentDbConnectionProperty.HOSTNAME.getName(),
                    hostName);
        }
    }

    private static void checkValidProperty(final String propertyKey) throws SQLException {
        // TODO: See if there should be additional checks.
        for (DocumentDbConnectionProperty property: DocumentDbConnectionProperty.values()) {
            if (property.getName().equals(propertyKey)) {
                return;
            }
        }
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_FAILURE,
                SqlError.UNSUPPORTED_PROPERTY,
                propertyKey
        );
    }

    private static void addPropertyIfNotSet(
            @NonNull final Properties info,
            @NonNull final String key,
            @Nullable final String value) {
        if (!isNullOrWhitespace(value)) {
            info.putIfAbsent(key, value);
        }
    }


    private boolean isExpectedJdbcScheme(@Nullable final String url) {
        return url != null && url.startsWith(DOCUMENT_DB_SCHEME);
    }

    @NonNull
    private String getMongoDbUrl(@NonNull final String url) {
        return url.substring(5).replaceFirst("^documentdb:", "mongodb:");
    }

    protected static boolean isNullOrWhitespace(@Nullable final String value) {
        return value == null || Pattern.matches("^\\s*$", value);
    }
}
