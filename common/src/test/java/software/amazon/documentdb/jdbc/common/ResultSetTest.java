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

package software.amazon.documentdb.jdbc.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.common.helpers.HelperFunctions;
import software.amazon.documentdb.jdbc.common.mock.MockConnection;
import software.amazon.documentdb.jdbc.common.mock.MockResultSet;
import software.amazon.documentdb.jdbc.common.mock.MockStatement;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Properties;

/**
 * Test for abstract ResultSet Object.
 */
public class ResultSetTest {
    private java.sql.ResultSet resultSet;
    private java.sql.Statement statement;

    @BeforeEach
    void initialize() {
        statement = new MockStatement(new MockConnection(new Properties()));
        resultSet = new MockResultSet(statement);
    }

    @Test
    @SuppressWarnings("deprecation")
    void testGetType() {
        HelperFunctions.expectFunctionThrows(() -> resultSet.getArray(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getArray(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getAsciiStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getAsciiStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBinaryStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBinaryStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBlob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBlob(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBoolean(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBoolean(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getByte(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getByte(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBytes(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBytes(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCharacterStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCharacterStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getClob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getClob(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDate(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDate(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDate(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDate("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDouble(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getDouble(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getFloat(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getFloat(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getInt(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getInt(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getLong(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getLong(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNCharacterStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNCharacterStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNClob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNClob(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNString(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNString(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(0, (Class<?>)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject("", (Class<?>)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(0, (Map<String, Class<?>>)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject("", (Map<String, Class<?>>)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRef(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRef(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRowId(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRowId(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getShort(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getShort(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getSQLXML(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getSQLXML(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getString(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getString(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTime(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTime(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTime(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTime("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTimestamp(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTimestamp(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTimestamp(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getTimestamp("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getUnicodeStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getUnicodeStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getURL(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getURL(""));
    }

    @Test
    void testUpdate() {
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateArray(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateArray("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBigDecimal(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBigDecimal("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null,  (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null,  (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, (Blob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", (Blob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, (InputStream)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", (InputStream)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBoolean(0, false));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBoolean("", false));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateByte(0, (byte)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateByte("", (byte)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBytes(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBytes("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null, (int)0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, (Clob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", (Clob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDate(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDate("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDouble(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDouble("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateFloat(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateFloat("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateInt(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateInt("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateLong(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateLong("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, (NClob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", (NClob)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNString(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNString("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNull(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNull(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRef(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRef("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRowId(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRowId("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateSQLXML(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateSQLXML("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateShort(0, (short) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateShort("", (short) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateString(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateString("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTime(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTime("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTimestamp(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTimestamp("", null));
    }

    @Test
    void testRow() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowDeleted(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowInserted(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowUpdated(), false);
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.relative(1), true);
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.relative(10), false);
        HelperFunctions.expectFunctionThrows(() -> resultSet.relative(-1));
        HelperFunctions.expectFunctionThrows(() -> resultSet.moveToCurrentRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.refreshRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.previous());
        HelperFunctions.expectFunctionThrows(() -> resultSet.insertRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.moveToInsertRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.deleteRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.cancelRowUpdates());
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isAfterLast(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isBeforeFirst(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isFirst(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isLast(), false);
        ((MockResultSet)resultSet).setRowIdx(-1);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isBeforeFirst(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isFirst(), false);
        ((MockResultSet)resultSet).setRowIdx(9);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isLast(), true);
        ((MockResultSet)resultSet).setRowIdx(10);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isAfterLast(), true);
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionThrows(() -> resultSet.first());
        HelperFunctions.expectFunctionThrows(() -> resultSet.last());
        HelperFunctions.expectFunctionThrows(() -> resultSet.beforeFirst());
        HelperFunctions.expectFunctionThrows(() -> resultSet.afterLast());
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getRow(), 1);
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(-1));
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(0));
        ((MockResultSet)resultSet).setRowIdx(10);
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(0));
        ((MockResultSet)resultSet).setRowIdx(0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.absolute(5), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.absolute(11), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getRow(), 0);
    }

    @Test
    void testFetch() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD));
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_REVERSE));
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_UNKNOWN));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getFetchDirection(), java.sql.ResultSet.FETCH_FORWARD);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getType(), java.sql.ResultSet.TYPE_FORWARD_ONLY);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getConcurrency(), java.sql.ResultSet.CONCUR_READ_ONLY);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCursorName());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getFetchSize(), 0);
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchSize(-1));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchSize(0));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchSize(1));
    }

    @Test
    void testGetStatement() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getStatement(), statement);
    }

    @Test
    void testWrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(MockResultSet.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(MockStatement.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.unwrap(MockResultSet.class), resultSet);
        HelperFunctions.expectFunctionThrows(() -> resultSet.unwrap(MockStatement.class));
    }

    @Test
    void testClosed() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isClosed(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.close());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isClosed(), true);
        HelperFunctions.expectFunctionThrows(() -> ((ResultSet)resultSet).verifyOpen());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.close());
    }

    @Test
    void testWarnings() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);

        HelperFunctions.expectFunctionDoesntThrow(() -> ((ResultSet)resultSet).addWarning(HelperFunctions.getNewWarning1()));
        final SQLWarning warning = HelperFunctions.getNewWarning1();
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), warning);
        warning.setNextWarning(HelperFunctions.getNewWarning2());
        HelperFunctions.expectFunctionDoesntThrow(() -> ((ResultSet)resultSet).addWarning(HelperFunctions.getNewWarning2()));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), warning);

        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);
    }
}
