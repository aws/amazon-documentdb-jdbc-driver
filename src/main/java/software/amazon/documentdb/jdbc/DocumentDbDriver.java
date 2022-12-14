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

import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public class DocumentDbDriver extends software.amazon.documentdb.jdbc.common.Driver {
    // Note: This class must be marked public for the registration/DeviceManager to work.
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbDriver.class);
    private static final String DRIVER_MAJOR_VERSION_KEY = "driver.major.version";
    private static final String DRIVER_MINOR_VERSION_KEY = "driver.minor.version";
    private static final String DRIVER_FULL_VERSION_KEY = "driver.full.version";
    private static final String DEFAULT_APPLICATION_NAME_KEY = "default.application.name";
    private static final String PROPERTIES_FILE_PATH = "/project.properties";
    static final int DRIVER_MAJOR_VERSION;
    static final int DRIVER_MINOR_VERSION;
    static final String DRIVER_VERSION;
    static final String DEFAULT_APPLICATION_NAME;

    // Registers the JDBC driver.
    static {
        // Retrieve driver metadata from properties file.
        int majorVersion = 0;
        int minorVersion = 0;
        String fullVersion = "";
        String defaultApplicationName = "";
        try (InputStream is = DocumentDbDatabaseMetaData.class.getResourceAsStream(PROPERTIES_FILE_PATH)) {
            final Properties p = new Properties();
            p.load(is);
            majorVersion = Integer.parseInt(p.getProperty(DRIVER_MAJOR_VERSION_KEY));
            minorVersion = Integer.parseInt(p.getProperty(DRIVER_MINOR_VERSION_KEY));
            fullVersion = p.getProperty(DRIVER_FULL_VERSION_KEY);
            defaultApplicationName = p.getProperty(DEFAULT_APPLICATION_NAME_KEY);
        } catch (Exception e) {
            LOGGER.error("Error loading driver version: " + e.getMessage());
        }
        DRIVER_MAJOR_VERSION = majorVersion;
        DRIVER_MINOR_VERSION = minorVersion;
        DRIVER_VERSION = fullVersion;
        DEFAULT_APPLICATION_NAME = defaultApplicationName;

        new DocumentDbDriver().register();
    }

    @SneakyThrows
    protected void register() {
        DriverManager.registerDriver(this);
    }

    @Override
    public @Nullable Connection connect(final @Nullable String url, final Properties info)
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

        return new DocumentDbConnection(properties);
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection to the given URL.
     * Typically drivers will return <code>true</code> if they understand the sub-protocol specified
     * in the URL and <code>false</code> if they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this driver understands the given URL;
     * <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or the url is {@code null}
     */
    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        if (url == null) {
            throw new SQLException("The url cannot be null");
        }

        return url.startsWith(getConnectStringPrefix());
    }

    protected String getConnectStringPrefix() {
        return DOCUMENT_DB_SCHEME;
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }
}
