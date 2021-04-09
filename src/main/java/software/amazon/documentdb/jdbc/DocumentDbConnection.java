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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.Connection;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * DocumentDb implementation of Connection.
 */
public class DocumentDbConnection extends Connection
        implements java.sql.Connection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbConnection.class.getName());

    private final DocumentDbConnectionProperties connectionProperties;
    private final String connectionId;
    private DocumentDbDatabaseMetaData metadata;
    private DocumentDbDatabaseSchemaMetadata databaseMetadata;
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;

    /**
     * DocumentDbConnection constructor, initializes super class.
     */
    DocumentDbConnection(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        super(connectionProperties);
        this.connectionProperties = connectionProperties;
        connectionId = UUID.randomUUID().toString();
        initializeClients(connectionProperties);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.INVALID_TIMEOUT,
                    timeout);
        }
        if (mongoDatabase != null) {
            try {
                // Convert to milliseconds
                final int maxTimeMS = timeout + 1000;
                pingDatabase(maxTimeMS);
                return true;
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public void doClose() {
        if (mongoDatabase != null) {
            mongoDatabase = null;
        }
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureDatabaseMetadata();
        return metadata;
    }

    private void ensureDatabaseMetadata() throws SQLException {
        if (metadata == null) {
            databaseMetadata = DocumentDbDatabaseSchemaMetadata
                            .get(connectionId, connectionProperties, true);
            metadata = new DocumentDbDatabaseMetaData(this, databaseMetadata, connectionProperties);
        }
    }

    DocumentDbDatabaseSchemaMetadata getDatabaseMetadata()
            throws SQLException {
        ensureDatabaseMetadata();
        return databaseMetadata;
    }

    @Override
    public String getSchema() {
        return connectionProperties.getDatabase();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO: Implement network timeout.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds)
            throws SQLException {
        // TODO: Implement network timeout.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType,
            final int resultSetConcurrency)
            throws SQLException {

        verifyOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }

        return new DocumentDbStatement(this);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        // TODO: Implement prepared statement.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isSupportedProperty(final String name) {
        return DocumentDbConnectionProperty.isSupportedProperty(name);
    }

    DocumentDbConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    private void initializeClients(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        // Create the mongo client.
        final MongoClientSettings settings = connectionProperties
                .buildMongoClientSettings();

        mongoClient = MongoClients.create(settings);
        mongoDatabase = mongoClient.getDatabase(connectionProperties.getDatabase());
        pingDatabase();
    }

    private void pingDatabase() throws SQLException {
        pingDatabase(0);
    }

    private void pingDatabase(final int maxTimeMS) throws SQLException {
        try {
            final String maxTimeMSOption = (maxTimeMS > 0)
                    ? String.format(", \"maxTimeMS\" : %d", maxTimeMS)
                    : "";
            mongoDatabase.runCommand(
                    Document.parse(String.format("{ \"ping\" : 1 %s }", maxTimeMSOption)));
        } catch (MongoSecurityException e) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    e,
                    SqlError.SECURITY_ERROR,
                    e.getMessage());
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }
}
