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

package software.amazon.documentdb.jdbc.common.utilities;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A data class to provide metadata for a result set column.
 */
@AllArgsConstructor
@Getter
public class JdbcColumnMetaData {

    /**
     * Creates a new {@link JdbcColumnMetaData}
     *
     * @param ordinal the zero-based ordinal of the column in the result set.
     * @param caseSensitive indicates whether a column's case matters.
     * @param nullable indicates the nullability of values in the designated column.
     * @param signed indicates whether values in the designated column are signed numbers.
     * @param columnDisplaySize indicates the designated column's normal maximum width in characters.
     * @param columnLabel the label of the column.
     * @param columnName the name of the column.
     * @param schemaName the schema the column belongs in.
     * @param precision the numeric precision.
     * @param scale the numeric scale.
     * @param columnType the column type.
     * @param columnTypeName the column type name.
     * @param columnClassName the column class name.
     */
    public JdbcColumnMetaData(
            final int ordinal,
            final boolean caseSensitive,
            final int nullable,
            final boolean signed,
            final int columnDisplaySize,
            final String columnLabel,
            final String columnName,
            final String schemaName,
            final int precision,
            final int scale,
            final int columnType,
            final String columnTypeName,
            final String columnClassName) {
        this(
                ordinal,
                false, //autoIncrement,
                caseSensitive,
                false, //searchable,
                false, //currency,
                nullable,
                signed,
                columnDisplaySize,
                columnLabel,
                columnName,
                schemaName,
                precision,
                scale,
                null, //tableName,
                null, //catalogName,
                columnType,
                columnTypeName,
                true, //readOnly,
                false, //writable,
                false, //definitelyWritable,
                columnClassName);
    }

    /** Gets the zero-based ordinal of the column in the result set. */
    private final int ordinal; // 0-based

    /** Indicates whether the designated column is automatically numbered. */
    private final boolean autoIncrement;

    /** Indicates whether a column's case matters. */
    private final boolean caseSensitive;

    /** Indicates whether the designated column can be used in a where clause. */
    private final boolean searchable;

    /** Indicates whether the designated column is a cash value. */
    private final boolean currency;

    /** Indicates the nullability of values in the designated column. */
    private final int nullable;

    /** Indicates whether values in the designated column are signed numbers. */
    private final boolean signed;

    /** Indicates the designated column's normal maximum width in characters. */
    private final int columnDisplaySize;

    /**
     * Gets the designated column's suggested title for use in printouts and displays. The suggested
     * title is usually specified by the SQL <code>AS</code> clause.  If a SQL <code>AS</code> is
     * not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     */
    private final String columnLabel;

    /** Get the designated column's name. */
    private final String columnName;

    /** Get the designated column's table's schema. */
    private final String schemaName;

    /**
     * Get the designated column's specified column size. For numeric data, this is the maximum
     * precision.  For character data, this is the length in characters. For datetime data types,
     * this is the length in characters of the String representation (assuming the maximum allowed
     * precision of the fractional seconds component). For binary data, this is the length in bytes.
     *  For the ROWID datatype, this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     */
    private final int precision;

    /**
     * Gets the designated column's number of digits to right of the decimal point. 0 is returned
     * for data types where the scale is not applicable.
     */
    private final int scale;

    /** Gets the designated column's table name. */
    private final String tableName;

    /** Gets the designated column's table's catalog name. */
    private final String catalogName;

    /** Retrieves the designated column's SQL type. */
    private final int columnType;

    /** Retrieves the designated column's database-specific type name. */
    private final String columnTypeName;

    /** Indicates whether the designated column is definitely not writable. */
    private final boolean readOnly;

    /** Indicates whether it is possible for a write on the designated column to succeed. */
    private final boolean writable;

    /** Indicates whether a write on the designated column will definitely succeed. */
    private final boolean definitelyWritable;

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code> is called to retrieve a value
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the class
     * returned by this method.
     */
    private final String columnClassName;
}
