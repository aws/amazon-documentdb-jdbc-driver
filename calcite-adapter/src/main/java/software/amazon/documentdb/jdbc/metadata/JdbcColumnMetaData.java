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

package software.amazon.documentdb.jdbc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;

import java.sql.Types;
import java.util.List;

/**
 * A data class to provide metadata for a result set column.
 */
@AllArgsConstructor
@Getter
public class JdbcColumnMetaData {
    private static final ImmutableMap<Integer, Rep> JDBC_TYPE_TO_REP;

    static {
        JDBC_TYPE_TO_REP = ImmutableMap.<Integer, Rep>builder()
                .put(Types.BIGINT, Rep.PRIMITIVE_LONG)
                .put(Types.BOOLEAN, Rep.PRIMITIVE_BOOLEAN)
                .put(Types.DECIMAL, Rep.NUMBER)
                .put(Types.DOUBLE, Rep.PRIMITIVE_DOUBLE)
                .put(Types.INTEGER, Rep.PRIMITIVE_INT)
                .put(Types.NULL, Rep.STRING)
                .put(Types.TIMESTAMP, Rep.JAVA_SQL_TIMESTAMP)
                .put(Types.VARCHAR, Rep.STRING)
                .put(Types.VARBINARY, Rep.BYTE_STRING)
                .build();
    }

    /**
     * Converts from list of {@link ColumnMetaData} to list of {@link JdbcColumnMetaData}.
     *
     * @param columnMetaData the list of column metadata.
     * @return a list of {@link JdbcColumnMetaData}.
     */
    public static List<JdbcColumnMetaData> fromCalciteColumnMetaData(final List<ColumnMetaData> columnMetaData) {
        final ImmutableList.Builder<JdbcColumnMetaData> builder = ImmutableList.builder();
        for (ColumnMetaData columnMetaDataItem : columnMetaData) {
            builder.add(fromCalciteColumnMetaData(columnMetaDataItem));
        }
        return builder.build();
    }

    /**
     * Converts a {@link ColumnMetaData} to a {@link JdbcColumnMetaData}.
     *
     * @param columnMetaData the column metadata.
     * @return a {@link JdbcColumnMetaData} instance.
     */
    public static JdbcColumnMetaData fromCalciteColumnMetaData(final ColumnMetaData columnMetaData) {
        return new JdbcColumnMetaData(
                columnMetaData.ordinal,
                columnMetaData.autoIncrement,
                columnMetaData.caseSensitive,
                columnMetaData.searchable,
                columnMetaData.currency,
                columnMetaData.nullable,
                columnMetaData.signed,
                columnMetaData.displaySize,
                columnMetaData.label,
                columnMetaData.columnName,
                columnMetaData.schemaName,
                columnMetaData.precision,
                columnMetaData.scale,
                columnMetaData.tableName,
                columnMetaData.catalogName,
                columnMetaData.type.id,
                columnMetaData.type.name,
                columnMetaData.readOnly,
                columnMetaData.writable,
                columnMetaData.definitelyWritable,
                columnMetaData.columnClassName);
    }

    /**
     * Converts a {@link JdbcColumnMetaData} to a {@link ColumnMetaData} instance.
     *
     * @return a {@link ColumnMetaData} instance.
     */
    public ColumnMetaData toCalciteColumnMetaData() {
        return new ColumnMetaData(
                ordinal,
                autoIncrement,
                caseSensitive,
                searchable,
                currency,
                nullable,
                signed,
                columnDisplaySize,
                columnLabel,
                columnName,
                schemaName,
                precision,
                scale,
                tableName,
                catalogName,
                new ColumnMetaData.AvaticaType(
                        columnType,columnTypeName, JDBC_TYPE_TO_REP.get(columnType)),
                readOnly,
                writable,
                definitelyWritable,
                columnClassName);
    }

    /** Gets the zero-based ordinal of the column in the result set. */
    private int ordinal; // 0-based

    /** Indicates whether the designated column is automatically numbered. */
    private boolean autoIncrement;

    /** Indicates whether a column's case matters. */
    private boolean caseSensitive;

    /** Indicates whether the designated column can be used in a where clause. */
    private boolean searchable;

    /** Indicates whether the designated column is a cash value. */
    private boolean currency;

    /** Indicates the nullability of values in the designated column. */
    private int nullable;

    /** Indicates whether values in the designated column are signed numbers. */
    private boolean signed;

    /** Indicates the designated column's normal maximum width in characters. */
    private int columnDisplaySize;

    /**
     * Gets the designated column's suggested title for use in printouts and displays. The suggested
     * title is usually specified by the SQL <code>AS</code> clause.  If a SQL <code>AS</code> is
     * not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     */
    private String columnLabel;

    /** Get the designated column's name. */
    private String columnName;

    /** Get the designated column's table's schema. */
    private String schemaName;

    /**
     * Get the designated column's specified column size. For numeric data, this is the maximum
     * precision.  For character data, this is the length in characters. For datetime data types,
     * this is the length in characters of the String representation (assuming the maximum allowed
     * precision of the fractional seconds component). For binary data, this is the length in bytes.
     *  For the ROWID datatype, this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     */
    private int precision;

    /**
     * Gets the designated column's number of digits to right of the decimal point. 0 is returned
     * for data types where the scale is not applicable.
     */
    private int scale;

    /** Gets the designated column's table name. */
    private String tableName;

    /** Gets the designated column's table's catalog name. */
    private String catalogName;

    /** Retrieves the designated column's SQL type. */
    private int columnType;

    /** Retrieves the designated column's database-specific type name. */
    private String columnTypeName;

    /** Indicates whether the designated column is definitely not writable. */
    private boolean readOnly;

    /** Indicates whether it is possible for a write on the designated column to succeed. */
    private boolean writable;

    /** Indicates whether a write on the designated column will definitely succeed. */
    private boolean definitelyWritable;

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code> is called to retrieve a value
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the class
     * returned by this method.
     */
    private String columnClassName;
}
