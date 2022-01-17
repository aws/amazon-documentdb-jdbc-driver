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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DocumentDbListResultSetTest {
    private static final int TEST_METADATA_SIZE = 3;

    @Mock
    private List<List<Object>> mockList;

    @Mock
    private JdbcColumnMetaData mockMetadataColumnA;

    @Mock
    private JdbcColumnMetaData mockMetadataColumnB;

    @Mock
    private JdbcColumnMetaData mockMetadataColumnC;

    @Mock
    private Statement mockStatement;

    private DocumentDbListResultSet resultSet;

    @BeforeEach
    void init() {
        MockitoAnnotations.openMocks(this);

        // Prepare mock metadata with bare minimum.
        Mockito.when(mockMetadataColumnA.getColumnLabel()).thenReturn("A");
        Mockito.when(mockMetadataColumnB.getColumnLabel()).thenReturn("B");
        Mockito.when(mockMetadataColumnC.getColumnLabel()).thenReturn("C");
        Mockito.when(mockMetadataColumnA.getOrdinal()).thenReturn(0);
        Mockito.when(mockMetadataColumnB.getOrdinal()).thenReturn(1);
        Mockito.when(mockMetadataColumnC.getOrdinal()).thenReturn(2);
        Mockito.when(mockList.size()).thenReturn(TEST_METADATA_SIZE);

        final ImmutableList<JdbcColumnMetaData> mockMetadata = ImmutableList
                .of(mockMetadataColumnA, mockMetadataColumnB, mockMetadataColumnC);
        resultSet = new DocumentDbListResultSet(mockStatement, mockMetadata, mockList);
    }

    @Test
    @DisplayName("Test that next() moves cursor to correct row and handles invalid inputs.")
    void testNext() throws SQLException {
        // Test cursor before first row.
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
        Assertions.assertEquals(3, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that absolute() moves cursor to correct row and handles invalid inputs.")
    void testAbsolute() throws SQLException {
        // Test going to valid row number with respect to start.
        Assertions.assertTrue(resultSet.absolute(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to valid row number with respect to end.
        Assertions.assertTrue(resultSet.absolute(-2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to out of range row numbers.
        Assertions.assertFalse(resultSet.absolute(4));
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
        Assertions.assertFalse(resultSet.absolute(-4));
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that relative() moves cursor to correct row and handles invalid inputs.")
    void testRelative() throws SQLException {
        // Test going to valid forward row number. (0 -> 2)
        Assertions.assertTrue(resultSet.relative(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to previous row number. (2 -> 1)
        Assertions.assertTrue(resultSet.relative(-1));
        Assertions.assertEquals(0, resultSet.getRowIndex());
        Assertions.assertEquals(1, resultSet.getRow());

        // Test staying in same row. (1 -> 1)
        Assertions.assertTrue(resultSet.relative(0));
        Assertions.assertEquals(0, resultSet.getRowIndex());
        Assertions.assertEquals(1, resultSet.getRow());

        // Test going to out of range row number. (1 -> 4)
        Assertions.assertFalse(resultSet.relative(3));
        Assertions.assertEquals(3, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());

        // Test going to out of range row number. (4 -> -1)
        Assertions.assertFalse(resultSet.relative(-5));
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that previous() moves cursor to correct row and handles invalid inputs.")
    void testPrevious() throws SQLException {
        // First go to valid forward row number. (0 -> 2)
        Assertions.assertTrue(resultSet.relative(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        // Test going to previous row number. (2 -> 1)
        Assertions.assertTrue(resultSet.previous());
        Assertions.assertEquals(0, resultSet.getRowIndex());
        Assertions.assertEquals(1, resultSet.getRow());

        // Test going to out of range row number. (1 -> 0)
        Assertions.assertFalse(resultSet.previous());
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that first() moves cursor to correct row.")
    void testFirst() throws SQLException {
        Assertions.assertTrue(resultSet.first());
        Assertions.assertEquals(0, resultSet.getRowIndex());
        Assertions.assertEquals(1, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that last() moves cursor to correct row.")
    void testLast() throws SQLException {
        Assertions.assertTrue(resultSet.last());
        Assertions.assertEquals(TEST_METADATA_SIZE - 1, resultSet.getRowIndex());
        Assertions.assertEquals(TEST_METADATA_SIZE, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that beforeFirst() moves cursor to correct row.")
    void testBeforeFirst() throws SQLException {
        // First go to valid forward row number. (0 -> 2)
        Assertions.assertTrue(resultSet.relative(2));
        Assertions.assertEquals(1, resultSet.getRowIndex());
        Assertions.assertEquals(2, resultSet.getRow());

        resultSet.beforeFirst();
        Assertions.assertEquals(-1, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that afterLast() moves cursor to correct row.")
    void testAfterLast() throws SQLException {
        resultSet.afterLast();
        Assertions.assertEquals(TEST_METADATA_SIZE, resultSet.getRowIndex());
        Assertions.assertEquals(0, resultSet.getRow());
    }

    @Test
    @DisplayName("Test that getType() returns scrollable and insensitive.")
    void testGetType() {
        Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, resultSet.getType());
    }

    @Test
    @DisplayName("Test that getConcurrency() returns read-only.")
    void testGetConcurrency() {
        Assertions.assertEquals(ResultSet.CONCUR_READ_ONLY, resultSet.getConcurrency());
    }
}
