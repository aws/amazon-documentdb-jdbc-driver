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

    // Registers the JDBC driver.
    static {
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
}
