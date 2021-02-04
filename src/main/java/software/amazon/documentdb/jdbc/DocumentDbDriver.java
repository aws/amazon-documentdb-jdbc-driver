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
import software.amazon.documentdb.jdbc.calcite.adapter.AbstractDocumentDbDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Provides a JDBC driver for the Amazon DocumentDB database.
 */
public class DocumentDbDriver extends AbstractDocumentDbDriver {
    // Note: This class must be marked public for the registration/DeviceManager to work.
    public static final String DOCUMENT_DB_SCHEME = "jdbc:documentdb:";

    // Registers the JDBC driver.
    static {
        new DocumentDbDriver().register();
    }

    @SneakyThrows
    @Override
    protected void register() {
        DriverManager.registerDriver(this);
    }

    @Override
    public @Nullable Connection connect(final @Nullable String url, final Properties info)
            throws SQLException {

        final Connection connection = super.connect(url, info);
        if (connection == null) {
            return null;
        }

        return new DocumentDbConnection(connection);
    }

    @Override
    protected String getConnectStringPrefix() {
        return DOCUMENT_DB_SCHEME;
    }
}
