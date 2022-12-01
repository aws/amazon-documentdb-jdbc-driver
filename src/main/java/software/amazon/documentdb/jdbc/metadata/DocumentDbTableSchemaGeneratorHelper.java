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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.bson.BsonType;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;

public class DocumentDbTableSchemaGeneratorHelper {
    static final String EMPTY_STRING = "";
    static final int KEY_COLUMN_NONE = 0;
    private static final String PATH_SEPARATOR = ".";
    private static final String ID_FIELD_NAME = "_id";
    private static final int ID_PRIMARY_KEY_COLUMN = 1;

    /**
     * The map of data type promotions.
     *
     * @see <a href="https://github.com/aws/amazon-documentdb-jdbc-driver#data-type-conflict-promotion">
     * Map Relational Schemas to DocumentDB - Scalar-Scalar Conflicts</a>
     */
    private static final ImmutableMap<Entry<JdbcType, BsonType>, JdbcType> PROMOTION_MAP =
            new ImmutableMap.Builder<Entry<JdbcType, BsonType>, JdbcType>()
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.BOOLEAN), JdbcType.BOOLEAN)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.DATE_TIME), JdbcType.TIMESTAMP)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.DOUBLE), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.INT32), JdbcType.INTEGER)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.INT64), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.TIMESTAMP), JdbcType.TIMESTAMP)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.NULL), JdbcType.NULL)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.ARRAY), JdbcType.ARRAY)
                    .put(new SimpleEntry<>(JdbcType.NULL, BsonType.DOCUMENT), JdbcType.JAVA_OBJECT)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.BOOLEAN), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.DECIMAL128), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.DOUBLE), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.INT32), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.INT64), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.NULL), JdbcType.ARRAY)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.ARRAY), JdbcType.ARRAY)
                    .put(new SimpleEntry<>(JdbcType.ARRAY, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.BOOLEAN), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.DECIMAL128), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.DOUBLE), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.INT32), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.INT64), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.NULL), JdbcType.JAVA_OBJECT)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.JAVA_OBJECT, BsonType.DOCUMENT), JdbcType.JAVA_OBJECT)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.BOOLEAN), JdbcType.BOOLEAN)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.DOUBLE), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.INT32), JdbcType.INTEGER)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.INT64), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.NULL), JdbcType.BOOLEAN)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BOOLEAN, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.BOOLEAN), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.DOUBLE), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.INT32), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.INT64), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.NULL), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.BIGINT, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.BOOLEAN), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.DOUBLE), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.INT32), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.INT64), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.NULL), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DECIMAL, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.BOOLEAN), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.DOUBLE), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.INT32), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.INT64), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.NULL), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.DOUBLE, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.BOOLEAN), JdbcType.INTEGER)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.DECIMAL128), JdbcType.DECIMAL)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.DOUBLE), JdbcType.DOUBLE)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.INT32), JdbcType.INTEGER)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.INT64), JdbcType.BIGINT)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.NULL), JdbcType.INTEGER)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.INTEGER, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.BOOLEAN), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.DATE_TIME), JdbcType.TIMESTAMP)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.DECIMAL128), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.DOUBLE), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.INT32), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.INT64), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.TIMESTAMP), JdbcType.TIMESTAMP)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.NULL), JdbcType.TIMESTAMP)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.TIMESTAMP, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.BOOLEAN), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.DATE_TIME), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.DECIMAL128), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.DOUBLE), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.INT32), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.INT64), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.TIMESTAMP), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.MAX_KEY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.MIN_KEY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.NULL), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.OBJECT_ID), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.STRING), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.ARRAY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARBINARY, BsonType.DOCUMENT), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.BOOLEAN), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.BINARY), JdbcType.VARBINARY)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.DATE_TIME), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.DECIMAL128), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.DOUBLE), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.INT32), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.INT64), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.TIMESTAMP), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.MAX_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.MIN_KEY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.NULL), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.OBJECT_ID), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.STRING), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.ARRAY), JdbcType.VARCHAR)
                    .put(new SimpleEntry<>(JdbcType.VARCHAR, BsonType.DOCUMENT), JdbcType.VARCHAR)
                    .build();

    /**
     * Combines two paths to form a new path.
     *
     * @param path      the root/parent path.
     * @param fieldName the field name to append to the path.
     * @return a new path with the fieldName append to the root path separated by a period.
     */
    public static String combinePath(final String path, final String fieldName) {
        final boolean isPathEmpty = Strings.isNullOrEmpty(path);
        final boolean isFieldNameEmpty = Strings.isNullOrEmpty(fieldName);
        final String pathSeparator = !isPathEmpty && !isFieldNameEmpty ? PATH_SEPARATOR : EMPTY_STRING;
        final String newPath = !isPathEmpty ? path : EMPTY_STRING;
        final String newFieldName = !isFieldNameEmpty ? fieldName : EMPTY_STRING;
        return String.format("%s%s%s", newPath, pathSeparator, newFieldName);
    }

    /**
     * Gets the promoted SQL data type from previous SQL data type and the current BSON data type.
     *
     * @param bsonType    the current BSON data type.
     * @param prevSqlType the previous SQL data type.
     * @return returns the promoted SQL data type.
     */
    @VisibleForTesting
    static JdbcType getPromotedSqlType(final BsonType bsonType, final JdbcType prevSqlType) {
        final Entry<JdbcType, BsonType> key = new SimpleEntry<>(prevSqlType, bsonType);
        return PROMOTION_MAP.getOrDefault(key, JdbcType.VARCHAR);
    }

    /**
     * Gets whether the field is the "_id" field.
     *
     * @param fieldName the name of the field.
     * @return returns {@code true} if the field name if "_id", {@code false} otherwise.
     */
    static boolean isIdField(final String fieldName) {
        return ID_FIELD_NAME.equals(fieldName);
    }

    /**
     * Handles a complex to scalar conflict by removing the previous table map and clearing existing
     * column map.
     *
     * @param tableMap  the table map.
     * @param path      the path to the table.
     * @param columnMap the column map.
     */
    static void handleComplexScalarConflict(
            final Map<String, DocumentDbSchemaTable> tableMap,
            final String path,
            final Map<String, DocumentDbSchemaColumn> columnMap) {
        tableMap.remove(path);
        columnMap.clear();
    }

    /**
     * Detects and handles the case were a conflict occurs at a lower lever in the array.
     * It removes the index column for the higher level array index.
     * It ensures the SQL type is set to VARCHAR.
     *
     * @param columnMap the column map to modify.
     * @param level     the current array level.
     * @param sqlType   the previous SQL type.
     * @return if a conflict is detected, returns VARCHAR, otherwise, the original SQL type.
     */
    static JdbcType handleArrayLevelConflict(
            final Map<String, DocumentDbSchemaColumn> columnMap,
            final int level,
            final JdbcType sqlType) {

        JdbcType newSqlType = sqlType;

        // Remove previously detect index columns at higher index level,
        // if we now have scalars at a lower index level.
        final Map<String, DocumentDbSchemaColumn> origColumns = new LinkedHashMap<>(columnMap);
        for (Entry<String, DocumentDbSchemaColumn> entry : origColumns.entrySet()) {
            final DocumentDbMetadataColumn column = (DocumentDbMetadataColumn) entry.getValue();
            if (column.getArrayIndexLevel() != null && column.getArrayIndexLevel() > level) {
                columnMap.remove(entry.getKey());
                // We're collapsing an array level, so revert to VARCHAR/VARBINARY this and for the higher
                // level array components.
                newSqlType = getPromotedSqlType(BsonType.STRING, newSqlType);
            }
        }

        return newSqlType;
    }

    /**
     * Gets the primary key column number depending on whether we are whether it is the primary key.
     *
     * @param isPrimaryKey an indicator of whether we are dealing with the primary key.
     * @return the value {@link #ID_PRIMARY_KEY_COLUMN} if the primary key, other it
     * returns {@link #KEY_COLUMN_NONE}.
     */
    static int getPrimaryKeyColumn(final boolean isPrimaryKey) {
        // If primary key, then first column, zero indicates not part of primary key.
        return isPrimaryKey ? ID_PRIMARY_KEY_COLUMN : KEY_COLUMN_NONE;
    }

    /**
     * Gets the field name, depending on whether it is the primary key.
     *
     * @param path the path the field belongs to.
     * @param fieldName the name of the field in the path.
     * @param isPrimaryKey an indicator of whether this is the primary key.
     * @param columnNameMap a map of unique column names.
     * @return a column name for the field.
     */
    static String getFieldNameIfIsPrimaryKey(
            final String path, final String fieldName,
            final boolean isPrimaryKey,
            final Map<String, String> columnNameMap) {
        return isPrimaryKey
                // For the primary key, qualify it with the parent name.
                ? toName(combinePath(getParentName(path), fieldName), columnNameMap)
                : fieldName;
    }

    /**
     * Gets the virtual table name, depending on whether this is the primary key.
     *
     * @param fieldPath the path the field belongs to.
     * @param nextSqlType the next SQL type.
     * @param isPrimaryKey an indicator of whether this is the primary key.
     * @param collectionName a map of unique column names.
     * @param tableNameMap a map of unique table names.
     * @return the name of the virtual table if not a primary key (base table) and type is not
     * ARRAY or JAVA_OBJECT. Otherwise, null.
     */
    static String getVirtualTableNameIfIsPrimaryKey(
            final String fieldPath,
            final JdbcType nextSqlType,
            final boolean isPrimaryKey,
            final String collectionName,
            final Map<String, String> tableNameMap) {
        return !isPrimaryKey && (nextSqlType == JdbcType.ARRAY || nextSqlType == JdbcType.JAVA_OBJECT)
                ? toName(combinePath(collectionName, fieldPath), tableNameMap)
                : null;
    }

    /**
     * Gets the SQL type, depending on whether this is the primary key.
     *
     * @param bsonType the underlying source data type.
     * @param prevSqlType the previous SQL type detected.
     * @param isPrimaryKey an indicator of whether this is the primary key.
     * @return the SQL type to use.
     */
    static JdbcType getSqlTypeIfIsPrimaryKey(
            final BsonType bsonType,
            final JdbcType prevSqlType,
            final boolean isPrimaryKey) {
        return isPrimaryKey && bsonType == BsonType.DOCUMENT
                ? JdbcType.VARCHAR
                : getPromotedSqlType(bsonType, prevSqlType);
    }

    /**
     * Gets the previous SQL data type.
     *
     * @param prevMetadataColumn the column to get the SQL type for. Can be null.
     * @return the previous SQL data type if the column is not null, {@link JdbcType#NULL},
     * otherwise.
     */
    static JdbcType getPrevSqlTypeOrDefault(
            final DocumentDbMetadataColumn prevMetadataColumn) {
        return prevMetadataColumn != null
                ? prevMetadataColumn.getSqlType()
                : JdbcType.NULL;
    }

    /**
     * Gets whether the given SQL type is a complex type (ARRAY or JAVA_OBJECT).
     *
     * @param sqlType the SQL type to tests.
     * @return {@code true} if a complex type, {@code false}, otherwise.
     */
    static boolean isComplexType(final JdbcType sqlType) {
        return sqlType == JdbcType.JAVA_OBJECT || sqlType == JdbcType.ARRAY;
    }

    /**
     * Adds to the list of primary keys, if is a primary key.
     *
     * @param foreignKeys the list of foreign keys.
     * @param isPrimaryKey an indicator of whether this is a primary key.
     * @param metadataColumn the column to add.
     */
    static void addToForeignKeysIfIsPrimary(
            final List<DocumentDbMetadataColumn> foreignKeys,
            final boolean isPrimaryKey,
            final DocumentDbMetadataColumn metadataColumn) {
        // Add the key to the foreign keys for child tables.
        if (isPrimaryKey) {
            foreignKeys.add(metadataColumn);
        }
    }

    /**
     * Gets the previous index for this column.
     *
     * @param prevMetadataColumn the previous column to use. Can be null.
     * @param defaultValue the default index value to use if the column is null.
     * @return the index of the column, if not null. Otherwise, the default value.
     */
    static int getPrevIndexOrDefault(final DocumentDbMetadataColumn prevMetadataColumn,
            final int defaultValue) {
        return prevMetadataColumn != null
                ? prevMetadataColumn.getIndex()
                : defaultValue;
    }

    /**
     * Checks and ensures consistency of SQL type between the primary key of the base table and any
     * generated virtual tables.
     *
     * @param tableMap the map of tables.
     * @param path the path of the collection.
     * @param columnMap the column map of the base table.
     * @param columnNameMap the map of unique column names.
     */
    static void checkVirtualTablePrimaryKeys(
            final Map<String, DocumentDbSchemaTable> tableMap,
            final String path,
            final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap,
            final Map<String, String> columnNameMap) {
        final String primaryKeyColumnName = toName(combinePath(path, ID_FIELD_NAME), columnNameMap);
        final DocumentDbMetadataColumn primaryKeyColumn = (DocumentDbMetadataColumn) columnMap
                .get(primaryKeyColumnName);
        for (DocumentDbSchemaTable table : tableMap.values()) {
            final DocumentDbMetadataColumn column = (DocumentDbMetadataColumn) table
                    .getColumnMap().get(primaryKeyColumnName);
            if (column != null && !column.getSqlType().equals(primaryKeyColumn.getSqlType())) {
                column.setSqlType(primaryKeyColumn.getSqlType());
            }
        }
    }

    /**
     * Converts the path to name swapping the period character for an underscore character. Unique
     * names of maximum length {@link org.apache.calcite.sql.parser.SqlParser#DEFAULT_IDENTIFIER_MAX_LENGTH}
     * are maintained in the uniqueNameMap parameter.
     *
     * @param path the path to convert.
     * @param uniqueNameMap the map of unique names.
     * @return a string the period character swapped for an underscore character, of correct maximum
     * length and unique within the map of given paths
     */
    @VisibleForTesting
    static String toName(final String path, final Map<String, String> uniqueNameMap) {
        return toName(path, uniqueNameMap, DEFAULT_IDENTIFIER_MAX_LENGTH);
    }

    /**
     * Converts the path to name swapping the period character for an underscore character. Unique
     * names of maximum length given in identifierMaxLength parameter. are maintained in the
     * uniqueNameMap parameter.
     *
     * @param path                the path to convert.
     * @param uniqueNameMap       the map of unique names.
     * @param identifierMaxLength the maximum length of identifier name.
     * @return a string the period character swapped for an underscore character, of correct maximum
     * length and unique within the map of given paths
     */
    @VisibleForTesting
    static String toName(
            final String path,
            final Map<String, String> uniqueNameMap,
            final int identifierMaxLength) {
        final String fullPathName = path.replaceAll("\\.", "_");
        // If already mapped, return the mapped value.
        if (uniqueNameMap.containsKey(path)) {
            return uniqueNameMap.get(path);
        }
        // If not greater the maximum allowed length, return value.
        if (path.length() <= identifierMaxLength) {
            return fullPathName;
        }

        // Shorten the name and ensure uniqueness.
        final StringBuilder shortenedName = new StringBuilder(fullPathName);
        final List<MatchResult> matches = getSeparatorMatches(path);
        if (matches.isEmpty()) {
            // Only "base table"
            shortenBaseName(
                    path,
                    uniqueNameMap,
                    identifierMaxLength,
                    shortenedName);
        } else if (matches.get(0).start() < identifierMaxLength) {
            // Base table shorter than max length - combine with trailing path.
            shortenWithBaseNameLessThanMaxLength(
                    path,
                    uniqueNameMap,
                    identifierMaxLength,
                    shortenedName,
                    matches);
        } else {
            // Base table too long. Combine on trailing path.
            shortenWithBaseNameLongerThanMaxLength(
                    path,
                    uniqueNameMap,
                    identifierMaxLength,
                    shortenedName,
                    matches);
        }
        return shortenedName.toString();
    }

    private static void shortenWithBaseNameLongerThanMaxLength(
            final String path,
            final Map<String, String> uniqueNameMap,
            final int identifierMaxLength,
            final StringBuilder shortenedName,
            final List<MatchResult> matches) {
        int lastMatchIndex = 0;
        for (int matchIndex = matches.size() - 1; matchIndex > 0; matchIndex--) {
            if ((path.length() - matches.get(matchIndex).start()) >= identifierMaxLength) {
                break;
            }
            lastMatchIndex = matchIndex;
        }
        if (lastMatchIndex > 0) {
            shortenedName.delete(0, matches.get(lastMatchIndex).start());
        } else {
            shortenedName.delete(0, shortenedName.length() - identifierMaxLength);
        }
        ensureUniqueName(uniqueNameMap, shortenedName, path);
    }

    private static void shortenWithBaseNameLessThanMaxLength(
            final String path,
            final Map<String, String> uniqueNameMap,
            final int identifierMaxLength,
            final StringBuilder shortenedName,
            final List<MatchResult> matches) {
        int lastMatchIndex = 0;
        for (int matchIndex = matches.size() - 1; matchIndex > 0; matchIndex--) {
            if ((path.length() - matches.get(matchIndex).start()) + matches.get(0).start()
                    >= identifierMaxLength) {
                break;
            }
            lastMatchIndex = matchIndex;
        }
        final int deleteChars;
        if (lastMatchIndex > 0) {
            deleteChars = matches.get(lastMatchIndex).start() - matches.get(0).start();
        } else {
            deleteChars = path.length() - identifierMaxLength;
        }
        shortenedName.delete(matches.get(0).start(), matches.get(0).start() + deleteChars);
        ensureUniqueName(uniqueNameMap, shortenedName, path);
    }

    private static void shortenBaseName(
            final String path,
            final Map<String, String> uniqueNameMap,
            final int identifierMaxLength,
            final StringBuilder shortenedName) {
        shortenedName.delete(identifierMaxLength, shortenedName.length());
        ensureUniqueName(uniqueNameMap, shortenedName, path);
    }

    private static void ensureUniqueName(
            final Map<String, String> uniqueNameMap,
            final StringBuilder shortenedName,
            final String path) {
        int counter = 0;
        final StringBuilder tempName = new StringBuilder(shortenedName);
        while (uniqueNameMap.values().stream().anyMatch(s -> tempName.toString().equals(s))) {
            counter++;
            final String counterString = String.valueOf(counter);
            tempName.setLength(0);
            tempName.append(
                    shortenedName.substring(0, shortenedName.length() - counterString.length()))
                    .append(counterString);
        }
        shortenedName.setLength(0);
        shortenedName.append(tempName);
        uniqueNameMap.put(path, shortenedName.toString());
    }

    private static List<MatchResult> getSeparatorMatches(final String path) {
        final List<MatchResult> matches = new ArrayList<>();
        final Pattern separatorPattern = Pattern.compile("\\.");
        final Matcher separatorMatcher = separatorPattern.matcher(path);
        separatorMatcher.reset();
        while (separatorMatcher.find()) {
            matches.add(separatorMatcher.toMatchResult());
        }
        return matches;
    }

    /**
     * Gets the parent name (last node) in the path.
     *
     * @param path the path to read.
     * @return the last node in the path.
     */
    private static String getParentName(final String path) {
        return path.substring(path.lastIndexOf('.') + 1);
    }
}
