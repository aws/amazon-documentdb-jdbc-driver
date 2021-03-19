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
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataScanner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public abstract class AbstractDocumentDbDriver extends Driver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDocumentDbDriver.class.getName());
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_EMPTY)
            .enable(SerializationFeature.INDENT_OUTPUT);

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
            // Get the properties and options of the URL.
            properties = DocumentDbConnectionProperties
                    .getPropertiesFromConnectionString(info, url, getConnectStringPrefix());
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
            throws JsonProcessingException {

        final JsonRoot rootModel = DocumentDbMetadataScanner.createViewModel(
                new DocumentDbConnectionProperties(properties));
        final String json = JSON_OBJECT_MAPPER.writeValueAsString(rootModel);
        properties.put("model", "inline:" + json);
    }
}
