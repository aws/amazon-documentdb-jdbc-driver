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

package software.amazon.documentdb.jdbc.calcite.adapter;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.model.JsonRoot;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperty;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataScanner;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public abstract class AbstractDocumentDbDriver extends Driver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDocumentDbDriver.class.getName());
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_EMPTY)
            .enable(SerializationFeature.INDENT_OUTPUT);
    private static final String MONGODB_SCHEME = "mongodb:";

    @Override
    protected abstract void register();

    @Override
    protected abstract String getConnectStringPrefix();

    @SneakyThrows
    @Override
    @Nullable
    public Connection connect(@Nullable final String url, final Properties info)
            throws SQLException {
        if (url == null || !acceptsURL(url)) {
            return null;
        }

        final DocumentDbConnectionProperties properties;
        try {
            // Let MongoDB driver check the properties and options of the URL.
            properties = getPropertiesFromConnectionString(info, getMongoDbUrl(url));
        } catch (IllegalArgumentException exception) {
            throw new SQLException(exception.getMessage(), exception);
        }

        setViewModel(properties);

        return super.connect(getConnectStringPrefix(), properties);
    }

    @Override
    public boolean acceptsURL(@NonNull final String url) throws SQLException {
        if (url == null) {
            throw new SQLException("The url cannot be null");
        }

        return super.acceptsURL(url);
    }

    @Override
    protected DriverVersion createDriverVersion() {
        // TODO: ensure this is set only once.
        return new DriverVersion(
                "Document DB JDBC Driver",
                "1.0-SNAPSHOT",
                "Document DB",
                "1.0-SNAPSHOT",
                true,
                1,
                0,
                4,
                0);
    }

    @Override
    protected Collection<ConnectionProperty> getConnectionProperties() {
        // TODO: Add DocumentDB connection properties.
        return super.getConnectionProperties();
    }

    @Override
    protected String getFactoryClassName(final JdbcVersion jdbcVersion) {
        // TODO: Implement custom factory class.
        return super.getFactoryClassName(jdbcVersion); // "org.apache.calcite.jdbc.DocumentDbJdbc41Factory";
    }

    private static void setViewModel(final DocumentDbConnectionProperties properties)
            throws JsonProcessingException, SQLException {

        final JsonRoot rootModel = DocumentDbMetadataScanner.createViewModel(
                new DocumentDbConnectionProperties(properties));
        final String json = JSON_OBJECT_MAPPER.writeValueAsString(rootModel);
        properties.put("model", "inline:" + json);
    }


    /**
     * Gets the connection properties from the connection string.
     * @param info the given properties.
     * @param mongoDbUrl the connection string.
     * @return a {@link DocumentDbConnectionProperties} with the properties set.
     * @throws SQLException if connection string is invalid.
     */
    @VisibleForTesting
    public static DocumentDbConnectionProperties getPropertiesFromConnectionString(
            final Properties info, final String mongoDbUrl)
            throws SQLException {

        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(info);
        final String postSchemeSuffix = mongoDbUrl.substring(MONGODB_SCHEME.length());
        if (!isNullOrWhitespace(postSchemeSuffix)) {
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
                        SqlError.INVALID_CONNECTION_PROPERTIES,
                        mongoDbUrl
                );
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }

        properties.validateRequiredProperties();
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

    @NonNull
    private String getMongoDbUrl(@NonNull final String url) {
        return url.substring(5).replaceFirst("^documentdb:", MONGODB_SCHEME);
    }
}
