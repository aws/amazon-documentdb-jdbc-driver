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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.Properties;
import java.util.regex.Pattern;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
class DocumentDbPreparedStatementTest extends DocumentDbFlapDoodleTest {

    private static final String DATABASE_NAME = "database";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String CONNECTION_STRING_TEMPLATE = "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanLimit=1000&scanMethod=%s";

    @BeforeAll
    static void initialize()  {
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

    /**
     * Tests that queries can be executed with PreparedStatement
     *
     * @throws SQLException if connection or query fails.
     */
    @Test
    @DisplayName("Tests that queries can be executed using PreparedStatement.")
    void testExecuteQuery() throws SQLException {
        final String collectionName = "testPreparedStatementBasicQuery";
        final String query = String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collectionName);
        final int recordCount = 10;
        prepareSimpleConsistentData(DATABASE_NAME, collectionName,
                recordCount, USER, PASSWORD);
        final String connectionString =
                String.format(
                        "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false",
                        USER, PASSWORD, getMongoPort(), DATABASE_NAME);
        final Connection connection = DriverManager.getConnection(connectionString);
        final PreparedStatement preparedStatement = new DocumentDbPreparedStatement(connection, query);
        final ResultSet resultSet = preparedStatement.executeQuery();
        try {
            int count = 0;
            while (resultSet.next()) {
                Assertions.assertTrue(
                        Pattern.matches("^\\w+$", resultSet.getString(collectionName + "__id")));
                Assertions.assertEquals(Double.MAX_VALUE, resultSet.getDouble("fieldDouble"));
                Assertions.assertEquals("新年快乐", resultSet.getString("fieldString"));
                Assertions.assertTrue(Pattern.matches("^\\w+$", resultSet.getString("fieldObjectId")));
                Assertions.assertTrue(resultSet.getBoolean("fieldBoolean"));
                Assertions.assertEquals(
                        Instant.parse("2020-01-01T00:00:00.00Z"),
                        resultSet.getTimestamp("fieldDate").toInstant());
                Assertions.assertEquals(Integer.MAX_VALUE, resultSet.getInt("fieldInt"));
                Assertions.assertEquals(Long.MAX_VALUE, resultSet.getLong("fieldLong"));
                Assertions.assertEquals("MaxKey", resultSet.getString("fieldMaxKey"));
                Assertions.assertEquals("MinKey", resultSet.getString("fieldMinKey"));
                Assertions.assertNull(resultSet.getString("fieldNull"));
                count++;
            }
            Assertions.assertEquals(recordCount, count);
        } finally {
            resultSet.close();
        }
    }


    /**
     * Tests that metadata can be retrieved before the query is executed.
     *
     * @throws SQLException if connection or query fails.
     */
    @Test
    @DisplayName("Tests getMetadata without querying.")
    void testGetMetadataQuery() throws SQLException {
        final String collectionName = "testPreparedStatementMetadata";
        final String query = String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collectionName);
        final int recordCount = 10;
        prepareSimpleConsistentData(DATABASE_NAME, collectionName,
                recordCount, USER, PASSWORD);
        final String connectionString =
                String.format(
                        "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false",
                        USER, PASSWORD, getMongoPort(), DATABASE_NAME);
        final Connection connection = DriverManager.getConnection(connectionString);
        final PreparedStatement preparedStatement = new DocumentDbPreparedStatement(connection, query);
        final ResultSetMetaData metadata = preparedStatement.getMetaData();
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(13, metadata.getColumnCount());
        Assertions.assertEquals(collectionName, metadata.getTableName(1));
        Assertions.assertNull(metadata.getCatalogName(1));
        Assertions.assertEquals(DATABASE_NAME, metadata.getSchemaName(1));

        Assertions.assertEquals(collectionName + "__id", metadata.getColumnName(1));
        Assertions.assertEquals(collectionName + "__id", metadata.getColumnLabel(1));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(1));
        Assertions.assertEquals("java.lang.String", metadata.getColumnClassName(1));
        Assertions.assertEquals(Types.VARCHAR, metadata.getColumnType(1));
        Assertions.assertEquals(0, metadata.isNullable(1));
        Assertions.assertEquals(65536, metadata.getPrecision(1));
        Assertions.assertEquals(65536, metadata.getColumnDisplaySize(1));

        Assertions.assertTrue(metadata.isReadOnly(1));
        Assertions.assertTrue(metadata.isSigned(1));
        Assertions.assertTrue(metadata.isCaseSensitive(1));
        Assertions.assertFalse(metadata.isWritable(1));
        Assertions.assertFalse(metadata.isAutoIncrement(1));
        Assertions.assertFalse(metadata.isCurrency(1));

        Assertions.assertEquals("fieldDouble", metadata.getColumnName(2));
        Assertions.assertEquals("DOUBLE", metadata.getColumnTypeName(2));
        Assertions.assertEquals(1, metadata.isNullable(2));
        Assertions.assertEquals(0, metadata.getScale(2));

        Assertions.assertEquals("fieldString", metadata.getColumnName(3));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(3));

        Assertions.assertEquals("fieldObjectId", metadata.getColumnName(4));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(4));

        Assertions.assertEquals("fieldBoolean", metadata.getColumnName(5));
        Assertions.assertEquals("BOOLEAN", metadata.getColumnTypeName(5));

        Assertions.assertEquals("fieldDate", metadata.getColumnName(6));
        Assertions.assertEquals("TIMESTAMP", metadata.getColumnTypeName(6));

        Assertions.assertEquals("fieldInt", metadata.getColumnName(7));
        Assertions.assertEquals("INTEGER", metadata.getColumnTypeName(7));

        Assertions.assertEquals("fieldLong", metadata.getColumnName(8));
        Assertions.assertEquals("BIGINT", metadata.getColumnTypeName(8));

        Assertions.assertEquals("fieldMaxKey", metadata.getColumnName(9));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(9));

        Assertions.assertEquals("fieldMinKey", metadata.getColumnName(10));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(10));

        Assertions.assertEquals("fieldNull", metadata.getColumnName(11));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(11));

        Assertions.assertEquals("fieldBinary", metadata.getColumnName(12));
        Assertions.assertEquals("VARBINARY", metadata.getColumnTypeName(12));

        Assertions.assertEquals("fieldDecimal128", metadata.getColumnName(13));
        Assertions.assertEquals("DECIMAL", metadata.getColumnTypeName(13));
    }

    private static String getJdbcConnectionString(final DocumentDbMetadataScanMethod method) {
        return String.format(
                CONNECTION_STRING_TEMPLATE,
                USER, PASSWORD, getMongoPort(), DATABASE_NAME, method.getName());
    }
 }
