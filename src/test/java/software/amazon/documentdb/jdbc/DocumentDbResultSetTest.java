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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbResultSetTest extends DocumentDbFlapDoodleTest {
    private static final int MOCK_FETCH_SIZE = 20;
    private static final String DATABASE_NAME = "resultDatabase";
    private static final String TEST_USER = "user";
    private static final String TEST_PASSWORD = "password";
    private static MongoClient client;
    private static Connection connection;
    private static Statement statement;
    private ResultSet resultSetFlapdoodle;

    @Mock
    private DocumentDbStatement mockStatement;

    @Mock
    private MongoCursor<Document> iterator;

    // Used for tests not involving getters.
    private static DocumentDbDatabaseSchemaMetadata emptyMetadata;

    private DocumentDbResultSet resultSet;

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    static void initialize() throws SQLException {
        // Add a valid users to the local MongoDB instance.
        client = createMongoClient("admin", "admin", "admin");
        createUser(DATABASE_NAME, TEST_USER, TEST_PASSWORD);

        emptyMetadata = new DocumentDbDatabaseSchemaMetadata(null, 0, ImmutableMap.of());
    }

    @BeforeEach
    void init() throws SQLException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockStatement.getFetchSize()).thenReturn(MOCK_FETCH_SIZE);
    }

    @AfterAll
    void close() throws SQLException {
        resultSetFlapdoodle.close();
        statement.close();
        connection.close();
    }

    @Test
    @DisplayName("Test that next() moves cursor to correct row and handles invalid inputs.")
    void testNext() throws SQLException {
        final Document doc1 = Document.parse("{\"_id\": \"key1\"}");
        final Document doc2 = Document.parse("{\"_id\": \"key2\"}");
        final Document doc3 = Document.parse("{\"_id\": \"key3\"}");
        Mockito.when(iterator.next()).thenReturn(doc1).thenReturn(doc2).thenReturn(doc3);
        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(mockStatement, iterator, ImmutableList.of(column), emptyMetadata);

        // Test cursor before first row.
        Mockito.when(iterator.hasNext()).thenReturn(true);
        Assertions.assertTrue(resultSet.isBeforeFirst());
        Assertions.assertFalse(resultSet.isFirst());
        Assertions.assertFalse(resultSet.isLast());
        Assertions.assertFalse(resultSet.isAfterLast());
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());

        // Test cursor at first row.
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.isBeforeFirst());
        Assertions.assertTrue(resultSet.isFirst());
        Assertions.assertFalse(resultSet.isLast());
        Assertions.assertFalse(resultSet.isAfterLast());
        Assertions.assertEquals(0, resultSet.getRowIndex());
        Assertions.assertEquals(1, resultSet.getRow());

        // Test cursor at second row.
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.isBeforeFirst());
        Assertions.assertFalse(resultSet.isFirst());
        Assertions.assertFalse(resultSet.isLast());
        Assertions.assertFalse(resultSet.isAfterLast());
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test cursor at last row.
        Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.isBeforeFirst());
        Assertions.assertFalse(resultSet.isFirst());
        Assertions.assertTrue(resultSet.isLast());
        Assertions.assertFalse(resultSet.isAfterLast());
        Assertions.assertEquals(2, resultSet.getRowIndex());
        Assertions.assertEquals(3, resultSet.getRow());

        // Test cursor after last row.
        Assertions.assertFalse(resultSet.next());
        Assertions.assertFalse(resultSet.isBeforeFirst());
        Assertions.assertFalse(resultSet.isFirst());
        Assertions.assertFalse(resultSet.isLast());
        Assertions.assertTrue(resultSet.isAfterLast());
        Assertions.assertEquals(2, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that absolute() moves cursor to correct row and handles invalid inputs.")
    void testAbsolute() throws SQLException {
        final Document doc1 = Document.parse("{\"_id\": \"key1\"}");
        final Document doc2 = Document.parse("{\"_id\": \"key2\"}");
        final Document doc3 = Document.parse("{\"_id\": \"key3\"}");
        Mockito.when(iterator.next()).thenReturn(doc1).thenReturn(doc2).thenReturn(doc3);
        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(mockStatement, iterator, ImmutableList.of(column), emptyMetadata);

        // Test going to negative row number. (0 -> -1)
        Mockito.when(iterator.hasNext()).thenReturn(true);
        Assertions.assertEquals(
                "The row value must be greater than 1.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.absolute(-1)).getMessage());
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());

        // Test going to valid row number. (0 -> 2)
        Assertions.assertTrue(resultSet.absolute(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to previous row number. (2 -> 1)
        Assertions.assertEquals(
                "Cannot retrieve previous rows.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.absolute(1)).getMessage());
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to out of range row number. (2 -> 4)
        Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        Assertions.assertFalse(resultSet.absolute(4));
        Assertions.assertEquals(2, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that relative() moves cursor to correct row and handles invalid inputs.")
    void testRelative() throws SQLException {
        final Document doc1 = Document.parse("{\"_id\": \"key1\"}");
        final Document doc2 = Document.parse("{\"_id\": \"key2\"}");
        final Document doc3 = Document.parse("{\"_id\": \"key3\"}");
        Mockito.when(iterator.next()).thenReturn(doc1).thenReturn(doc2).thenReturn(doc3);
        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(mockStatement, iterator, ImmutableList.of(column), emptyMetadata);

        // Test going to valid row number. (0 -> 2)
        Mockito.when(iterator.hasNext()).thenReturn(true);
        Assertions.assertTrue(resultSet.relative(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to previous row number. (2 -> 1)
        Assertions.assertEquals(
                "Cannot retrieve previous rows.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.relative(-1)).getMessage());
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test staying in same row. (2 -> 2)
        Assertions.assertTrue(resultSet.relative(0));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to out of range row number. (2 -> 4)
        Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        Assertions.assertFalse(resultSet.relative(2));
        Assertions.assertEquals(2, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that close() closes the Mongo cursor and result set.")
    void testClose() throws SQLException {
        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(mockStatement, iterator, ImmutableList.of(column), emptyMetadata);

        // Test close.
        Assertions.assertDoesNotThrow(() -> resultSet.close());
        Assertions.assertTrue(resultSet.isClosed());
        Mockito.verify(iterator, Mockito.times(1)).close();

        // Attempt to close twice.
        Assertions.assertDoesNotThrow(() -> resultSet.close());
        Assertions.assertTrue(resultSet.isClosed());

        // Attempt to use closed result set.
        Assertions.assertEquals(
                "ResultSet is closed.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.next()).getMessage());
    }

    @Test
    @DisplayName("Tests that findColumn() returns the correct 1-based column index.")
    void testFindColumn() throws SQLException {
        final JdbcColumnMetaData column1 =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        final JdbcColumnMetaData column2 =
                JdbcColumnMetaData.builder().columnLabel("value").ordinal(1).build();
        final JdbcColumnMetaData column3 =
                JdbcColumnMetaData.builder().columnLabel("Value").ordinal(2).build();

        final ImmutableList<JdbcColumnMetaData> columnMetaData =
                ImmutableList.of(column1, column2, column3);
        resultSet = new DocumentDbResultSet(mockStatement, iterator, columnMetaData, emptyMetadata);

        Assertions.assertEquals(2, resultSet.findColumn("value"));
        Assertions.assertEquals(3, resultSet.findColumn("Value"));
        Assertions.assertEquals(
                String.format("Unknown column label: %s", "value2"),
                Assertions.assertThrows(SQLException.class, () -> resultSet.findColumn("value2"))
                        .getMessage());
    }

    @Test
    @DisplayName("Tests that fetch size can be set and get successfully.")
    void testGetAndSetFetchSize() throws SQLException {
        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        final ImmutableList<JdbcColumnMetaData> columnMetaData = ImmutableList.of(column);
        resultSet = new DocumentDbResultSet(mockStatement, iterator, columnMetaData, emptyMetadata);

        Assertions.assertEquals(MOCK_FETCH_SIZE, resultSet.getFetchSize());
        Assertions.assertDoesNotThrow(() -> resultSet.setFetchSize(10));
        Assertions.assertEquals(10, resultSet.getFetchSize());
    }

    @DisplayName("Test verifyRow and verifyColumnIndex")
    void testVerifyRowVerifyColumnIndex() throws SQLException {
        final Document doc1 = Document.parse("{\"_id\": null }");

        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(mockStatement, iterator, ImmutableList.of(column), emptyMetadata);

        // Try access before first row.
        Assertions.assertEquals("Result set before first row.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.getString(1))
                        .getMessage());

        // Move to first row.
        Mockito.when(iterator.hasNext()).thenReturn(true);
        Mockito.when(iterator.next()).thenReturn(doc1);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(Assertions.assertDoesNotThrow(() -> resultSet.getString(1)));
        Assertions.assertEquals("Invalid index (2), indexes must be between 1 and 1 (inclusive).",
                Assertions.assertThrows(SQLException.class, () -> resultSet.getString(2))
                        .getMessage());
        Assertions.assertEquals("Invalid index (0), indexes must be between 1 and 1 (inclusive).",
                Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0))
                        .getMessage());
        Assertions.assertEquals("Invalid index (-1), indexes must be between 1 and 1 (inclusive).",
                Assertions.assertThrows(SQLException.class, () -> resultSet.getString(-1))
                        .getMessage());

        // Move past last row.
        Mockito.when(iterator.hasNext()).thenReturn(false);
        Assertions.assertFalse(resultSet.next());
        Assertions.assertEquals("Result set after last row.",
                Assertions.assertThrows(SQLException.class, () -> resultSet.getString(1))
                        .getMessage());
    }

    @Test
    @DisplayName("Tests get from string")
    void testGetString() throws SQLException {
        final String collection = "resultSetTestString";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        document.append("field", new BsonString("30"));
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals("30", resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Tests get from int")
    void testGetInt() throws SQLException {
        final String collection = "resultSetTestInt";
        final Document document = Document.parse("{\"_id\": \"key1\", \"field\": 3}");
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new BigDecimal("3"), resultSetFlapdoodle.getBigDecimal(2));
        Assertions.assertEquals(3, resultSetFlapdoodle.getDouble(2), 0.01);
        Assertions.assertEquals(3, resultSetFlapdoodle.getFloat(2), 0.01);
        Assertions.assertEquals(3, resultSetFlapdoodle.getInt(2));
        Assertions.assertEquals(3, resultSetFlapdoodle.getLong(2));
        Assertions.assertEquals(3, resultSetFlapdoodle.getShort(2));
    }

    @Test
    @DisplayName("Tests get from double")
    void testGetDouble() throws SQLException {
        final String collection = "resultSetTestDouble";
        final Document document = Document.parse("{\"_id\": \"key1\", \"field\": 1.5}");
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new BigDecimal("1.5"), resultSetFlapdoodle.getBigDecimal(2));
        Assertions.assertEquals(1.5, resultSetFlapdoodle.getDouble(2), 0.01);
        Assertions.assertEquals(1.5, resultSetFlapdoodle.getFloat(2), 0.01);
        Assertions.assertEquals(1, resultSetFlapdoodle.getInt(2));
        Assertions.assertEquals(1, resultSetFlapdoodle.getLong(2));
        Assertions.assertEquals(1, resultSetFlapdoodle.getShort(2));
        Assertions.assertEquals("1.5", resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Tests get from int64")
    void testGetInt64() throws SQLException {
        final String collection = "resultSetTestInt64";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        document.append("field", new BsonInt64(1000000000000L));
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new BigDecimal("1000000000000"), resultSetFlapdoodle.getBigDecimal(2));
        Assertions.assertEquals(1000000000000d, resultSetFlapdoodle.getDouble(2), 1);
        Assertions.assertEquals(1000000000000f, resultSetFlapdoodle.getFloat(2), 1000);
        Assertions.assertEquals(0, resultSetFlapdoodle.getInt(2)); // getInt returns default value 0 if result > max value
        Assertions.assertEquals(1000000000000L, resultSetFlapdoodle.getLong(2));
        Assertions.assertEquals(0, resultSetFlapdoodle.getShort(2)); // getShort returns default value 0 if result > max value
        Assertions.assertEquals("1000000000000", resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Tests get from null")
    void testGetNull() throws SQLException {
        final String collection = "resultSetTestNull";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        document.append("field", new BsonNull());
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new BigDecimal("0"), resultSetFlapdoodle.getBigDecimal(2));
        Assertions.assertEquals(0, resultSetFlapdoodle.getDouble(2), 1);
        Assertions.assertEquals(0, resultSetFlapdoodle.getFloat(2), 1000);
        Assertions.assertEquals(0, resultSetFlapdoodle.getInt(2));
        Assertions.assertEquals(0, resultSetFlapdoodle.getLong(2));
        Assertions.assertEquals(0, resultSetFlapdoodle.getShort(2));
        Assertions.assertNull(resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Tests that getters from nested documents work.")
    void testGetNested() throws SQLException {
        final String collection = "resultSetTestNested";
        final Document document = Document.parse("{\"_id\": \"key1\", " +
                "\"extraField\": \"string\"," +
                "\"subdocument\": " +
                "{\"field\": 4}}");
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection + "_subdocument"));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new BigDecimal("4"), resultSetFlapdoodle.getBigDecimal(2));
        Assertions.assertEquals(4, resultSetFlapdoodle.getDouble(2), 0.1);
        Assertions.assertEquals(4, resultSetFlapdoodle.getFloat(2), 0.1);
        Assertions.assertEquals(4, resultSetFlapdoodle.getInt(2));
        Assertions.assertEquals(4L, resultSetFlapdoodle.getLong(2));
        Assertions.assertEquals(4, resultSetFlapdoodle.getShort(2));
        Assertions.assertEquals("4", resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Test for get from ObjectId")
    void testGetId() throws SQLException {
        final String collection = "resultSetTestId";
        final Document document = new Document();
        final ObjectId id = new ObjectId();
        document.append("_id", id);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(id.toString(), resultSetFlapdoodle.getString(1));
    }

    @Test
    @DisplayName("Test for get from Boolean")
    void testGetBoolean() throws SQLException {
        final String collection = "resultSetTestBoolean";
        final Document document = Document.parse("{\"_id\": \"key1\", \"field\": false}");
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals("false", resultSetFlapdoodle.getString(2));
        Assertions.assertFalse(resultSetFlapdoodle.getBoolean(2));
    }

    @Test
    @DisplayName("Test for get from Date")
    void testGetDate() throws SQLException {
        final String collection = "resultSetTestDate";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        final BsonDateTime date = new BsonDateTime(100000);
        document.append("date", date);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(new Date(date.getValue()), resultSetFlapdoodle.getDate(2));
        Assertions.assertEquals(new Timestamp(date.getValue()), resultSetFlapdoodle.getTimestamp(2));
        Assertions.assertEquals(new Time(date.getValue()), resultSetFlapdoodle.getTime(2));
    }

    @Test
    @DisplayName("Test for get from Regex")
    void testGetRegex() throws SQLException {
        final String collection = "resultSetTestRegex";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        final BsonRegularExpression regex = new BsonRegularExpression("^example");
        document.append("regex", regex);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(regex.toString(), resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Test for get from Min/Max key")
    void testGetMinMaxKey() throws SQLException {
        final String collection = "resultSetTestMinMax";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        final BsonMaxKey max = new BsonMaxKey();
        final BsonMinKey min = new BsonMinKey();
        document.append("max", max);
        document.append("min", min);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals("MaxKey", resultSetFlapdoodle.getString(2));
        Assertions.assertEquals("MinKey", resultSetFlapdoodle.getString(3));
    }

    @Test
    @DisplayName("Test for get from timestamp")
    void testGetTimestamp() throws SQLException {
        final String collection = "resultSetTestTimestamp";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        final BsonTimestamp timestamp = new BsonTimestamp(100000);
        document.append("timestamp", timestamp);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertEquals(timestamp.toString(), resultSetFlapdoodle.getString(2));
    }

    @Test
    @DisplayName("Test for get from binary")
    void testGetBinary() throws SQLException {
        final String collection = "resultSetTestBinary";
        final Document document = Document.parse("{\"_id\": \"key1\"}");
        final BsonBinary binary = new BsonBinary("123abc".getBytes(StandardCharsets.UTF_8));
        document.append("binary", binary);
        client.getDatabase(DATABASE_NAME).getCollection(collection).insertOne(document);
        connection = DriverManager.getConnection(String.format(
                "jdbc:documentdb://%s:%s@localhost:%s/%s?tls=false&scanMethod=%s",
                TEST_USER, TEST_PASSWORD, getMongoPort(), DATABASE_NAME, DocumentDbMetadataScanMethod.ALL.getName()));
        statement = connection.createStatement();
        resultSetFlapdoodle = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection));
        Assertions.assertTrue(resultSetFlapdoodle.next());
        Assertions.assertArrayEquals(binary.getData(), resultSetFlapdoodle.getBytes(2));
        Assertions.assertArrayEquals(binary.getData(), resultSetFlapdoodle.getBlob(2).getBytes(1,6));

    }
}
