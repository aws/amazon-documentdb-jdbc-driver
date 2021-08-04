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

package software.amazon.documentdb.jdbc.common.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonType;

import java.sql.SQLException;
import java.util.UUID;

public class DocumentDbDocumentDbTestEnvironment extends DocumentDbAbstractTestEnvironment {

    private static final int DEFAULT_PORT = 27019;
    private static final String DOC_DB_HOST_LOCAL = "localhost";
    private static final String DOC_DB_USER_NAME_PROPERTY = "DOC_DB_USER_NAME";
    private static final String DOC_DB_PASSWORD_PROPERTY = "DOC_DB_PASSWORD";
    private static final String DOC_DB_LOCAL_PORT_PROPERTY = "DOC_DB_LOCAL_PORT";

    private static final String DOC_DB_CONNECTION_OPTIONS = "?tls=true&tlsAllowInvalidHostnames=true&scanMethod=random";
    private static final String DOC_DB_INTEGRATION_DATABASE = "integration";
    private static final String RESTRICTED_USERNAME = "docDbRestricted";

    private String databaseName;
    private final int port;

    DocumentDbDocumentDbTestEnvironment() {
        super(DOC_DB_HOST_LOCAL,
                System.getenv(DOC_DB_USER_NAME_PROPERTY),
                System.getenv(DOC_DB_PASSWORD_PROPERTY),
                RESTRICTED_USERNAME,
                DOC_DB_CONNECTION_OPTIONS);
        databaseName = null;
        port = getInteger(System.getenv(DOC_DB_LOCAL_PORT_PROPERTY), DEFAULT_PORT);
    }

    @Override
    protected boolean startEnvironment() {
        databaseName = UUID.randomUUID().toString();
        return false;
    }

    @Override
    protected boolean stopEnvironment() {
        try (MongoClient client = createMongoClient()) {
            final MongoDatabase database = client.getDatabase(getDatabaseName());
            database.runCommand(BsonDocument.parse("{ \"dropDatabase\": 1 }"));
        } catch (SQLException ex) {
            // Ignore
        } finally {
            databaseName = null;
        }
        return false;
    }

    @Override
    protected int getPort() {
        return port;
    }

    @Override
    protected boolean isBsonTypeCompatible(final BsonType bsonType) {
        switch (bsonType) {
            case DB_POINTER:
            case DECIMAL128:
            case END_OF_DOCUMENT:
            case JAVASCRIPT:
            case JAVASCRIPT_WITH_SCOPE:
            case SYMBOL:
            case UNDEFINED:
                return false;
            default:
                return true;
        }
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return DocumentDbDocumentDbTestEnvironment.class.getSimpleName() + "{" +
                " databaseName='" + databaseName + '\'' +
                ", username='" + getUsername() + '\'' +
                ", port=" + port +
                ", enableAuthentication=" + true +
                " }";
    }

    private static Integer getInteger(final String value, final Integer defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
