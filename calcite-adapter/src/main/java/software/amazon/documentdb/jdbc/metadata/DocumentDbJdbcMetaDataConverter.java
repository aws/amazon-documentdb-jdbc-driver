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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import java.sql.Types;
import java.util.List;

public class DocumentDbJdbcMetaDataConverter {
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
}
