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
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter;

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
    private static final String COLLECTION_NAME = "testCollection";
    private static final String QUERY =  String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME);
    private static final int RECORD_COUNT = 10;

    @BeforeAll
    static void initialize()  {
        // Add a valid users to the local MongoDB instance.
        createUser(DATABASE_NAME, USER, PASSWORD);
        prepareSimpleConsistentData(DATABASE_NAME, COLLECTION_NAME, RECORD_COUNT, USER, PASSWORD);
    }

    @AfterEach
    void afterEach() throws Exception {
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(
                        new Properties(),
                        getJdbcConnectionString(),
                        "jdbc:documentdb:");
        try (DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(properties, null)) {
            schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
        }
    }

    /**
     * Tests that queries can be executed with PreparedStatement
     *
     * @throws SQLException if connection or query fails.
     */
    @Test
    @DisplayName("Tests that queries can be executed using PreparedStatement.")
    void testExecuteQuery() throws SQLException {
        final Connection connection = DriverManager.getConnection(getJdbcConnectionString());
        final PreparedStatement preparedStatement = new DocumentDbPreparedStatement(connection, QUERY);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            int count = 0;
            while (resultSet.next()) {
                Assertions.assertTrue(
                        Pattern.matches("^\\w+$", resultSet.getString(COLLECTION_NAME + "__id")));
                Assertions.assertEquals(Double.MAX_VALUE, resultSet.getDouble("fieldDouble"));
                Assertions.assertEquals("新年快乐", resultSet.getString("fieldString"));
                Assertions.assertTrue(
                        Pattern.matches("^\\w+$", resultSet.getString("fieldObjectId")));
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
            Assertions.assertEquals(RECORD_COUNT, count);
        }
    }


    /**
     * Tests that metadata can be retrieved before the query is executed and
     * that it matches after execution.
     *
     * @throws SQLException if connection or query fails.
     */
    @Test
    @DisplayName("Tests getMetadata without querying and after querying.")
    void testGetMetadataQueryBeforeExecute() throws SQLException {
        final Connection connection = DriverManager.getConnection(getJdbcConnectionString());
        final PreparedStatement preparedStatement = new DocumentDbPreparedStatement(connection, QUERY);
        // Check the metadata.
        checkMetadata(preparedStatement.getMetaData());
        // Execute the statement.
        preparedStatement.execute();
        // Check the metadata again.
        checkMetadata(preparedStatement.getMetaData());
    }

    private static void checkMetadata(final ResultSetMetaData resultSetMetaData) throws SQLException {
        Assertions.assertNotNull(resultSetMetaData);
        Assertions.assertEquals(13, resultSetMetaData.getColumnCount());
        Assertions.assertEquals(COLLECTION_NAME, resultSetMetaData.getTableName(1));
        Assertions.assertNull(resultSetMetaData.getCatalogName(1));
        Assertions.assertEquals(DATABASE_NAME, resultSetMetaData.getSchemaName(1));

        Assertions.assertEquals(COLLECTION_NAME + "__id", resultSetMetaData.getColumnName(1));
        Assertions.assertEquals(COLLECTION_NAME + "__id", resultSetMetaData.getColumnLabel(1));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(1));
        Assertions.assertEquals("java.lang.String", resultSetMetaData.getColumnClassName(1));
        Assertions.assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
        Assertions.assertEquals(0, resultSetMetaData.isNullable(1));
        Assertions.assertEquals(65536, resultSetMetaData.getPrecision(1));
        Assertions.assertEquals(65536, resultSetMetaData.getColumnDisplaySize(1));

        Assertions.assertTrue(resultSetMetaData.isReadOnly(1));
        Assertions.assertTrue(resultSetMetaData.isSigned(1));
        Assertions.assertTrue(resultSetMetaData.isCaseSensitive(1));
        Assertions.assertFalse(resultSetMetaData.isWritable(1));
        Assertions.assertFalse(resultSetMetaData.isAutoIncrement(1));
        Assertions.assertFalse(resultSetMetaData.isCurrency(1));

        Assertions.assertEquals("fieldDouble", resultSetMetaData.getColumnName(2));
        Assertions.assertEquals("DOUBLE", resultSetMetaData.getColumnTypeName(2));
        Assertions.assertEquals(1, resultSetMetaData.isNullable(2));
        Assertions.assertEquals(0, resultSetMetaData.getScale(2));

        Assertions.assertEquals("fieldString", resultSetMetaData.getColumnName(3));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(3));

        Assertions.assertEquals("fieldObjectId", resultSetMetaData.getColumnName(4));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(4));

        Assertions.assertEquals("fieldBoolean", resultSetMetaData.getColumnName(5));
        Assertions.assertEquals("BOOLEAN", resultSetMetaData.getColumnTypeName(5));

        Assertions.assertEquals("fieldDate", resultSetMetaData.getColumnName(6));
        Assertions.assertEquals("TIMESTAMP", resultSetMetaData.getColumnTypeName(6));

        Assertions.assertEquals("fieldInt", resultSetMetaData.getColumnName(7));
        Assertions.assertEquals("INTEGER", resultSetMetaData.getColumnTypeName(7));

        Assertions.assertEquals("fieldLong", resultSetMetaData.getColumnName(8));
        Assertions.assertEquals("BIGINT", resultSetMetaData.getColumnTypeName(8));

        Assertions.assertEquals("fieldMaxKey", resultSetMetaData.getColumnName(9));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(9));

        Assertions.assertEquals("fieldMinKey", resultSetMetaData.getColumnName(10));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(10));

        Assertions.assertEquals("fieldNull", resultSetMetaData.getColumnName(11));
        Assertions.assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(11));

        Assertions.assertEquals("fieldBinary", resultSetMetaData.getColumnName(12));
        Assertions.assertEquals("VARBINARY", resultSetMetaData.getColumnTypeName(12));

        Assertions.assertEquals("fieldDecimal128", resultSetMetaData.getColumnName(13));
        Assertions.assertEquals("DECIMAL", resultSetMetaData.getColumnTypeName(13));
    }

    @Test
    @DisplayName("Tests getting and setting query timeout")
    void testGetSetQueryTimeout() throws SQLException {
        final Connection connection = DriverManager.getConnection(getJdbcConnectionString());
        final PreparedStatement preparedStatement = new DocumentDbPreparedStatement(connection, QUERY);
        Assertions.assertDoesNotThrow(() -> preparedStatement.setQueryTimeout(30));
        Assertions.assertEquals(30, preparedStatement.getQueryTimeout());
    }

    private static String getJdbcConnectionString() {
        return String.format(
                CONNECTION_STRING_TEMPLATE,
                USER, PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.RANDOM.getName());
    }
 }
