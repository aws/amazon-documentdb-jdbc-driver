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
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import java.sql.SQLException;

public class DocumentDbResultSetTest {
    private static final int MOCK_FETCH_SIZE = 20;

    @Mock private DocumentDbStatement statement;

    @Mock private MongoCursor<Document> iterator;

    private DocumentDbResultSet resultSet;

    @BeforeEach
    void init() throws SQLException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(statement.getFetchSize()).thenReturn(MOCK_FETCH_SIZE);
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
                new DocumentDbResultSet(statement, iterator, ImmutableList.of(column));

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
                new DocumentDbResultSet(statement, iterator, ImmutableList.of(column));

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
                new DocumentDbResultSet(statement, iterator, ImmutableList.of(column));

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
                new DocumentDbResultSet(statement, iterator, ImmutableList.of(column));

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
        resultSet = new DocumentDbResultSet(statement, iterator, columnMetaData);

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
        resultSet = new DocumentDbResultSet(statement, iterator, columnMetaData);

        Assertions.assertEquals(MOCK_FETCH_SIZE, resultSet.getFetchSize());
        Assertions.assertDoesNotThrow(() -> resultSet.setFetchSize(10));
        Assertions.assertEquals(10, resultSet.getFetchSize());
    }

    @Test
    @DisplayName("Test verifyRow and verifyColumnIndex")
    void testVerifyRowVerifyColumnIndex() throws SQLException {
        final Document doc1 = Document.parse("{\"_id\": null }");

        final JdbcColumnMetaData column =
                JdbcColumnMetaData.builder().columnLabel("_id").ordinal(0).build();
        resultSet =
                new DocumentDbResultSet(statement, iterator, ImmutableList.of(column));

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
}
