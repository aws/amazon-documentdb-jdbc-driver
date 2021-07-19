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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
class DocumentDbStatementTest extends DocumentDbFlapDoodleTest {

    protected static final String DATABASE_NAME = "database";
    protected static final String USER = "user";
    protected static final String PASSWORD = "password";
    protected static final String CONNECTION_STRING_TEMPLATE = "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanLimit=1000&scanMethod=%s";

    @BeforeAll
    static void initialize() {
        // Add a valid users to the local MongoDB instance.
        createUser(DATABASE_NAME, USER, PASSWORD);
    }

    @AfterEach
    void afterEach() throws SQLException {
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(
                        new Properties(),
                        getJdbcConnectionString(DocumentDbMetadataScanMethod.RANDOM),
                        "jdbc:documentdb:");
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties);
        schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
    }

    protected static DocumentDbStatement getDocumentDbStatement() throws SQLException {
        return getDocumentDbStatement(DocumentDbMetadataScanMethod.RANDOM);
    }

    protected static DocumentDbStatement getDocumentDbStatement(final DocumentDbMetadataScanMethod method) throws SQLException {
        final String connectionString = getJdbcConnectionString(method);
        final Connection connection = DriverManager.getConnection(connectionString);
        Assertions.assertNotNull(connection);
        final DocumentDbStatement statement = (DocumentDbStatement) connection.createStatement();
        Assertions.assertNotNull(statement);
        return statement;
    }

    protected static String getJdbcConnectionString(final DocumentDbMetadataScanMethod method) {
        return String.format(
                CONNECTION_STRING_TEMPLATE,
                USER, PASSWORD, getMongoPort(), DATABASE_NAME, method.getName());
    }
}
