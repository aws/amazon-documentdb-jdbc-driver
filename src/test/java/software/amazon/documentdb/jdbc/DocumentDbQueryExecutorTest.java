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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaException;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static software.amazon.documentdb.jdbc.DocumentDbStatementTest.getDocumentDbStatement;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryExecutorTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String TEST_USER = "user";
    private static final String TEST_PASSWORD = "password";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES = new DocumentDbConnectionProperties();

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    static void initialize() {
        // Add a valid users to the local MongoDB instance.
        createUser(DATABASE_NAME, TEST_USER, TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setUser(TEST_USER);
        VALID_CONNECTION_PROPERTIES.setPassword(TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setDatabase(DATABASE_NAME);
        VALID_CONNECTION_PROPERTIES.setTlsEnabled("false");
        VALID_CONNECTION_PROPERTIES.setHostname("localhost:" + getMongoPort());
    }

    @AfterEach
    void afterEach() throws SQLException {
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(VALID_CONNECTION_PROPERTIES);
        schemaWriter.remove("id");
    }

    /**
     * Lifted from DocumentDbResultSetTest but uses the DocumentDbQueryExecutor to get a result.
     * Shows that we are getting the correct result set metadata without the Avatica driver.
     */
    @Test
    void testGetResultSetMetadataSimple() throws SQLException, DocumentDbSchemaException {
        final String collectionSimple = "collectionSimple";
        prepareSimpleConsistentData(DATABASE_NAME, collectionSimple,
                5, TEST_USER, TEST_PASSWORD);
        final DocumentDbDatabaseSchemaMetadata databaseMetadata = DocumentDbDatabaseSchemaMetadata
                .get(VALID_CONNECTION_PROPERTIES, "id", true);
        final DocumentDbQueryMappingService queryMapper = new DocumentDbQueryMappingService(
                VALID_CONNECTION_PROPERTIES, databaseMetadata);
        final DocumentDbStatement statement = getDocumentDbStatement();
        final DocumentDbQueryExecutor queryExecutor = new DocumentDbQueryExecutor(
                statement,
                null,
                queryMapper,
                0,
                0);
        final ResultSet resultSet = queryExecutor.executeQuery(String.format(
                "SELECT * FROM \"%s\"", collectionSimple));
        Assertions.assertNotNull(resultSet);

        final ResultSetMetaData metadata = resultSet.getMetaData();
        Assertions.assertEquals(13, metadata.getColumnCount());
        Assertions.assertEquals(collectionSimple, metadata.getTableName(1));
        Assertions.assertNull(metadata.getCatalogName(1));
        Assertions.assertEquals(DATABASE_NAME, metadata.getSchemaName(1));

        Assertions.assertEquals(collectionSimple + "__id", metadata.getColumnName(1));
        Assertions.assertEquals(collectionSimple + "__id", metadata.getColumnLabel(1));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(1));
        Assertions.assertEquals("java.lang.String", metadata.getColumnClassName(1));
        Assertions.assertEquals(Types.VARCHAR,  metadata.getColumnType(1));
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
}
