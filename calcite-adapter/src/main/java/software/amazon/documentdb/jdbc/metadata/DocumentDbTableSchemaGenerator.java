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

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.EMPTY_STRING;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.KEY_COLUMN_NONE;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.addToForeignKeysIfIsPrimary;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.checkVirtualTablePrimaryKeys;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.combinePath;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getFieldNameIfIsPrimaryKey;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getPrevIndexOrDefault;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getPrevSqlTypeOrDefault;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getPrimaryKeyColumn;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getPromotedSqlType;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getSqlTypeIfIsPrimaryKey;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getVirtualTableNameIfIsPrimaryKey;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.handleArrayLevelConflict;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.handleComplexScalarConflict;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.isComplexType;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.isIdField;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.toName;

/**
 * Represents the fields in a collection and their data types. A collection can be broken up into
 * one (the base table) or more tables (virtual tables from embedded documents or arrays).
 */
@Getter
public class DocumentDbTableSchemaGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbTableSchemaGenerator.class);
    private static final String INDEX_COLUMN_NAME_PREFIX = "index_lvl_";
    private static final String VALUE_COLUMN_NAME = "value";

    /**
     * Creates new collection metadata for a given collection from the provided data.
     *
     * @param collectionName the name of the collection this model should refer.
     * @param cursor         the cursor for the data from which to create a model.
     * @return a new {@link DocumentDbTableSchemaGenerator} built from the data.
     */
    public static Map<String, DocumentDbSchemaTable> generate(
            final String collectionName,
            final Iterator<BsonDocument> cursor) {
        final LinkedHashMap<String, DocumentDbSchemaTable> tableMap = new LinkedHashMap<>();
        final Map<String, String> tableNameMap = new HashMap<>();
        while (cursor.hasNext()) {
            final BsonDocument document = cursor.next();
            processDocument(document, tableMap, new ArrayList<>(),
                    EMPTY_STRING, collectionName, true, tableNameMap);
        }

        // Remove array and document columns that are used for interim processing.
        filterArrayAndDocumentColumns(tableMap);

        return tableMap;
    }

    private static void filterArrayAndDocumentColumns(final LinkedHashMap<String, DocumentDbSchemaTable> tableMap) {
        for (DocumentDbSchemaTable table : tableMap.values()) {
            final boolean needsUpdate = table.getColumnMap().values().stream()
                    .anyMatch(c -> c.getSqlType() == JdbcType.ARRAY || c.getSqlType() == JdbcType.JAVA_OBJECT);
            if (needsUpdate) {
                final LinkedHashMap<String, DocumentDbSchemaColumn> columns = table
                        .getColumnMap().values().stream()
                        .filter(c -> c.getSqlType() != JdbcType.ARRAY && c.getSqlType() != JdbcType.JAVA_OBJECT)
                        .collect(Collectors.toMap(
                                DocumentDbSchemaColumn::getSqlName,
                                c -> c,
                                (o, d) -> o,
                                LinkedHashMap::new));
                if (LOGGER.isDebugEnabled() && !tableMap.containsKey(table.getSqlName())) {
                    LOGGER.debug(String.format("Added schema for table %s.", table.getSqlName()));
                }
                tableMap.put(table.getSqlName(), DocumentDbMetadataTable.builder()
                        .sqlName(table.getSqlName())
                        .collectionName(table.getCollectionName())
                        .columns(columns)
                        .build());
            }
        }
    }

    /**
     * Process a document including fields, sub-documents and arrays.
     *
     * @param document    the document to process.
     * @param tableMap    the map of virtual tables
     * @param foreignKeys the list of foreign keys.
     * @param path        the path for this field.
     * @param tableNameMap the map of table path to (shortened) names.
     */
    private static void processDocument(
            final BsonDocument document,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final String collectionName,
            final boolean isRootDocument,
            final Map<String, String> tableNameMap) {

        // Need to preserve order of fields.
        final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap = new LinkedHashMap<>();

        final String tableName = toName(combinePath(collectionName, path), tableNameMap);
        if (tableMap.containsKey(tableName)) {
            // If we've already visited this document/table,
            // start with the previously discovered columns.
            // This will have included and primary/foreign key definitions.
            columnMap.putAll(tableMap.get(tableName).getColumnMap());
        } else {
            // Add foreign keys.
            //
            // Foreign key(s) are the primary key(s) passed from the parent table.
            // Minimally, this is the primary key for the "_id" field.
            //
            // If called from an array parent, it will also include the "index_lvl_<n>"
            // column(s) from the previous level in the array.
            //
            // The primaryKeyColumn and foreignKeyColumn are the one-indexed value
            // referencing the order withing the primary or foreign key column.
            int primaryKeyColumn = KEY_COLUMN_NONE;
            for (DocumentDbMetadataColumn column : foreignKeys) {
                primaryKeyColumn++;
                buildForeignKeysFromDocument(columnMap, tableName, primaryKeyColumn, column);
            }
        }

        final Map<String, String> columnNameMap = columnMap.values().stream().collect(
                Collectors.toMap(
                        DocumentDbSchemaColumn::getSqlName,
                        DocumentDbSchemaColumn::getSqlName));
        // Process all fields in the document
        for (Entry<String, BsonValue> entry : document.entrySet()) {
            final String fieldName = entry.getKey();
            final String fieldPath = combinePath(path, fieldName);
            final BsonValue bsonValue = entry.getValue();
            final BsonType bsonType = bsonValue.getBsonType();
            final boolean isPrimaryKey = isRootDocument && isIdField(fieldName);
            final String columnName = getFieldNameIfIsPrimaryKey(
                    collectionName, fieldName, isPrimaryKey, columnNameMap);
            final DocumentDbMetadataColumn prevMetadataColumn = (DocumentDbMetadataColumn) columnMap
                    .getOrDefault(columnName, null);

            // ASSUMPTION: relying on the behaviour that the "_id" field will ALWAYS be first
            // in the root document.
            final JdbcType prevSqlType = getPrevSqlTypeOrDefault(prevMetadataColumn);
            final JdbcType nextSqlType = getSqlTypeIfIsPrimaryKey(bsonType, prevSqlType, isPrimaryKey);
            if (LOGGER.isDebugEnabled()) {
                final JdbcType currentDocType = getSqlTypeIfIsPrimaryKey(bsonType, JdbcType.NULL, isPrimaryKey);
                if (!prevSqlType.equals(currentDocType) && prevMetadataColumn != null) {
                    LOGGER.debug(String.format("Type conflict in table %s, types %s and %s mapped to %s.",
                            tableName, prevSqlType.name(), currentDocType, nextSqlType.name()));
                }
            }

            processComplexTypes(
                    tableMap,
                    new ArrayList<>(foreignKeys),
                    collectionName,
                    entry,
                    fieldPath,
                    bsonType,
                    prevMetadataColumn,
                    nextSqlType,
                    tableNameMap);
            final DocumentDbMetadataColumn metadataColumn = DocumentDbMetadataColumn
                    .builder()
                    .fieldPath(fieldPath)
                    .sqlName(columnName)
                    .sqlType(nextSqlType)
                    .dbType(bsonType)
                    .isIndex(false)
                    .isPrimaryKey(isPrimaryKey)
                    .index(getPrevIndexOrDefault(prevMetadataColumn, columnMap.size() + 1))
                    .tableName(tableName)
                    .primaryKeyIndex(getPrimaryKeyColumn(isPrimaryKey))
                    .foreignKeyIndex(KEY_COLUMN_NONE)
                    .isGenerated(false)
                    .virtualTableName(getVirtualTableNameIfIsPrimaryKey(
                            fieldPath, nextSqlType, isPrimaryKey, collectionName, tableNameMap))
                    .build();
            columnMap.put(metadataColumn.getSqlName(), metadataColumn);
            addToForeignKeysIfIsPrimary(foreignKeys, isPrimaryKey, metadataColumn);
        }

        // Ensure virtual table primary key column data types are consistent.
        if (isRootDocument) {
            checkVirtualTablePrimaryKeys(tableMap, collectionName, columnMap, columnNameMap);
        }

        // Add virtual table.
        final DocumentDbMetadataTable metadataTable = DocumentDbMetadataTable
                .builder()
                .sqlName(tableName)
                .collectionName(collectionName)
                .columns(columnMap)
                .build();
        if (LOGGER.isDebugEnabled() && !tableMap.containsKey(metadataTable.getSqlName())) {
            LOGGER.debug(String.format("Added schema for table %s.", metadataTable.getSqlName()));
        }
        tableMap.put(metadataTable.getSqlName(), metadataTable);
    }

    /**
     * Processes an array field, including sub-documents, and sub-arrays.
     *
     * @param array          the array value to process.
     * @param tableMap       the map of virtual tables
     * @param foreignKeys    the list of foreign keys.
     * @param path           the path for this field.
     * @param arrayLevel     the zero-indexed level of the array.
     * @param collectionName the name of the collection.
     * @param tableNameMap   the map of table path to (shortened) names.
     */
    private static void processArray(
            final BsonArray array,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final int arrayLevel,
            final String collectionName,
            final Map<String, String> tableNameMap) {

        // Need to preserve order of fields.
        final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap = new LinkedHashMap<>();

        int primaryKeyColumn = KEY_COLUMN_NONE;
        int level = arrayLevel;
        DocumentDbMetadataColumn metadataColumn;

        JdbcType prevSqlType = JdbcType.NULL;
        JdbcType sqlType;
        final String tableName = toName(combinePath(collectionName, path), tableNameMap);

        if (tableMap.containsKey(tableName)) {
            // If we've already visited this document/table,
            // start with the previously discovered columns.
            // This will have included and primary/foreign key definitions.
            columnMap.putAll(tableMap.get(tableName).getColumnMap());
            final String valueColumnPath = VALUE_COLUMN_NAME;
            // TODO: Figure out if previous type was array of array.
            if (columnMap.containsKey(toName(valueColumnPath, tableNameMap))) {
                prevSqlType = columnMap.get(toName(valueColumnPath, tableNameMap)).getSqlType();
            } else {
                prevSqlType = JdbcType.JAVA_OBJECT;
            }
        }

        // Find the promoted SQL data type for all elements.
        sqlType = prevSqlType;
        for (BsonValue element : array) {
            sqlType = getPromotedSqlType(element.getBsonType(), sqlType);
            if (LOGGER.isDebugEnabled()) {
                final JdbcType currentSqlType = getPromotedSqlType(element.getBsonType(), JdbcType.NULL);
                if (!prevSqlType.equals(currentSqlType)) {
                    LOGGER.debug(String.format("Type conflict in array table %s, types %s and %s mapped to %s.",
                            tableName, prevSqlType.name(), currentSqlType, sqlType.name()));
                }
            }
        }
        if (!isComplexType(sqlType)) {
            if (isComplexType(prevSqlType)) {
                // If promoted to scalar type from complex type, remove previous definition.
                handleComplexScalarConflict(tableMap, tableName, columnMap);
            } else {
                // Check to see if we're processing scalars at a different level than previously
                // detected.
                sqlType = handleArrayLevelConflict(columnMap, level, sqlType);
            }
        } else if (isComplexType(sqlType) && !isComplexType(prevSqlType)) {
            // Promoted from NULL to ARRAY or OBJECT.
            handleComplexScalarConflict(tableMap, tableName, columnMap);
        }

        if (!tableMap.containsKey(path)) {
            // Add foreign keys.
            //
            // Foreign key(s) are the primary key(s) passed from the parent table.
            // Minimally, this is the primary key for the "_id" field.
            //
            // If called from an array parent, it will also include the "index_lvl_<n>"
            // column(s) from the previous level in the array.
            //
            // The primaryKeyColumn and foreignKeyColumn are the one-indexed value
            // referencing the order withing the primary or foreign key column.
            for (DocumentDbMetadataColumn column : foreignKeys) {
                primaryKeyColumn++;
                metadataColumn = DocumentDbMetadataColumn
                        .builder()
                        .fieldPath(column.getFieldPath())
                        .sqlName(column.getSqlName())
                        .sqlType(column.getSqlType())
                        .dbType(column.getDbType())
                        .isIndex(column.isIndex())
                        .isPrimaryKey(primaryKeyColumn != 0)
                        .foreignKeyTableName(column.getTableName().equals(tableName)
                                ? null
                                : column.getTableName())
                        .index(column.getIndex())
                        .tableName(tableName)
                        .primaryKeyIndex(primaryKeyColumn)
                        .foreignKeyIndex(column.getTableName().equals(tableName)
                                ? KEY_COLUMN_NONE
                                : primaryKeyColumn)
                        .virtualTableName(column.getVirtualTableName())
                        .arrayIndexLevel(column.getArrayIndexLevel())
                        .isGenerated(column.isGenerated())
                        .build();
                metadataColumn.setForeignKeyColumnName(metadataColumn.getForeignKeyTableName() != null
                        ? column.getSqlName()
                        : null);
                columnMap.put(metadataColumn.getSqlName(), metadataColumn);
            }

            final Map<String, String> columnNameMap = columnMap.values().stream().collect(
                    Collectors.toMap(
                            DocumentDbSchemaColumn::getSqlName,
                            DocumentDbSchemaColumn::getSqlName));
            final String indexColumnName = toName(
                    combinePath(path, INDEX_COLUMN_NAME_PREFIX + level),
                    columnNameMap);
            if (!columnMap.containsKey(indexColumnName)) {
                // Add index column. Although it has no path in the original document, we will
                // use the path of the generated index field once the original document is unwound.
                primaryKeyColumn++;
                metadataColumn = DocumentDbMetadataColumn
                        .builder()
                        .sqlName(indexColumnName)
                        .fieldPath(path)  // Once unwound, the index will be at root level so path = name.
                        .sqlType(JdbcType.BIGINT)
                        .isIndex(true)
                        .isPrimaryKey(true)
                        .index(columnMap.size() + 1)
                        .tableName(tableName)
                        .primaryKeyIndex(primaryKeyColumn)
                        .foreignKeyIndex(KEY_COLUMN_NONE)
                        .arrayIndexLevel(level)
                        .isGenerated(true)
                        .build();
                columnMap.put(metadataColumn.getSqlName(), metadataColumn);
                foreignKeys.add(metadataColumn);
            }
        }

        // Add documents, arrays or just the scalar value.
        switch (sqlType) {
            case JAVA_OBJECT:
                processDocumentsInArray(array,
                        tableMap,
                        foreignKeys,
                        path,
                        collectionName,
                        tableNameMap);
                break;
            case ARRAY:
                // This will add another level to the array.
                level++;
                processArrayInArray(array,
                        tableMap,
                        foreignKeys,
                        path,
                        collectionName,
                        level,
                        tableNameMap);
                break;
            default:
                processValuesInArray(
                        tableMap,
                        path,
                        collectionName,
                        columnMap,
                        sqlType,
                        tableNameMap);
                break;
        }
    }

    /**
     * Processes value elements as a value column.
     *
     * @param tableMap       the table map of virtual tables.
     * @param path           the path to this array
     * @param collectionName the name of the collection.
     * @param columnMap      the map of columns for this virtual table.
     * @param sqlType        the promoted SQL data type to use for this array.
     * @param tableNameMap   the map of table path to (shortened) names.
     */
    private static void processValuesInArray(
            final Map<String, DocumentDbSchemaTable> tableMap,
            final String path,
            final String collectionName,
            final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap,
            final JdbcType sqlType,
            final Map<String, String> tableNameMap) {

        final String tableName = toName(combinePath(collectionName, path), tableNameMap);
        final Map<String, String> columnNameMap = columnMap.values().stream().collect(
                Collectors.toMap(
                        DocumentDbSchemaColumn::getSqlName,
                        DocumentDbSchemaColumn::getSqlName));
        // Get column if it already exists so we can preserve index order.
        final String valueColumnName = toName(VALUE_COLUMN_NAME, columnNameMap);
        final DocumentDbMetadataColumn prevMetadataColumn = (DocumentDbMetadataColumn) columnMap
                .get(valueColumnName);
        // Add value column
        final DocumentDbMetadataColumn metadataColumn = DocumentDbMetadataColumn
                .builder()
                .fieldPath(path)
                .sqlName(valueColumnName)
                .sqlType(sqlType)
                .isIndex(false)
                .isPrimaryKey(false)
                .index(getPrevIndexOrDefault(prevMetadataColumn, columnMap.size() + 1))
                .tableName(tableName)
                .primaryKeyIndex(KEY_COLUMN_NONE)
                .foreignKeyIndex(KEY_COLUMN_NONE)
                .isGenerated(false)
                .build();
        columnMap.put(metadataColumn.getSqlName(), metadataColumn);
        final DocumentDbMetadataTable metadataTable = DocumentDbMetadataTable
                .builder()
                .sqlName(tableName)
                .collectionName(collectionName)
                .columns(columnMap)
                .build();
        if (LOGGER.isDebugEnabled() && !tableMap.containsKey(metadataTable.getSqlName())) {
            LOGGER.debug(String.format("Added schema for table %s.", metadataTable.getSqlName()));
        }
        tableMap.put(metadataTable.getSqlName(), metadataTable);
    }

    /**
     * Processes array elements within an array.
     *
     * @param array          the array elements to scan.
     * @param tableMap       the table map of virtual tables.
     * @param foreignKeys    the list of foreign keys.
     * @param path           the path to this array
     * @param collectionName the name of the collection.
     * @param level          the current level of this array.
     * @param tableNameMap   the map of table path to (shortened) names.
     */
    private static void processArrayInArray(
            final BsonArray array,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final String collectionName,
            final int level,
            final Map<String, String> tableNameMap) {
        for (BsonValue element : array) {
            processArray(
                    element.asArray(),
                    tableMap,
                    foreignKeys,
                    path,
                    level,
                    collectionName,
                    tableNameMap);
        }
    }

    /**
     * Processes document elements in an array.
     *
     * @param array          the array elements to scan.
     * @param tableMap       the table map of virtual tables.
     * @param foreignKeys    the list of foreign keys.
     * @param path           the path to this array
     * @param collectionName the name of the collection encountered.
     * @param tableNameMap   the map of table path to (shortened) names.
     */
    private static void processDocumentsInArray(
            final BsonArray array,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final String collectionName,
            final Map<String, String> tableNameMap) {

        // This will make the document fields part of this array.
        for (BsonValue element : array) {
            processDocument(element.asDocument(),
                    tableMap, foreignKeys, path, collectionName, false, tableNameMap);
        }
    }

    private static void processComplexTypes(
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String collectionName,
            final Entry<String, BsonValue> entry,
            final String fieldPath,
            final BsonType bsonType,
            final DocumentDbMetadataColumn prevMetadataColumn,
            final JdbcType nextSqlType,
            final Map<String, String> tableNameMap) {
        if (nextSqlType == JdbcType.JAVA_OBJECT && bsonType != BsonType.NULL) {
            // This will create/update virtual table.
            processDocument(entry.getValue().asDocument(),
                    tableMap, foreignKeys, fieldPath, collectionName, false, tableNameMap);

        } else if (nextSqlType == JdbcType.ARRAY && bsonType != BsonType.NULL) {
            // This will create/update virtual table.
            processArray(entry.getValue().asArray(),
                    tableMap, foreignKeys, fieldPath, 0, collectionName, tableNameMap);
        } else {
            // Process a scalar data type.
            if (prevMetadataColumn != null && prevMetadataColumn.getVirtualTableName() != null
                    && bsonType != BsonType.NULL) {
                // This column has been promoted to a scalar type from a complex type.
                // Remove the previously defined virtual table.
                tableMap.remove(prevMetadataColumn.getVirtualTableName());
            }
        }
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private static void buildForeignKeysFromDocument(
            final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap,
            final String tableName,
            final int primaryKeyColumn,
            final DocumentDbMetadataColumn column) {
        final DocumentDbMetadataColumn metadataColumn = DocumentDbMetadataColumn
                .builder()
                .fieldPath(column.getFieldPath())
                .sqlName(column.getSqlName())
                .sqlType(column.getSqlType())
                .dbType(column.getDbType())
                .isIndex(column.isIndex())
                .isPrimaryKey(column.isPrimaryKey())
                .foreignKeyTableName(column.getTableName().equals(tableName)
                        ? null
                        : column.getTableName())
                .foreignKeyColumnName(column.getTableName().equals(tableName)
                        ? null
                        : column.getSqlName())
                .index(columnMap.size() + 1)
                .tableName(column.getTableName())
                .primaryKeyIndex(primaryKeyColumn)
                .foreignKeyIndex(column.getTableName().equals(tableName)
                        ? KEY_COLUMN_NONE
                        : primaryKeyColumn)
                .arrayIndexLevel(column.getArrayIndexLevel())
                .isGenerated(column.isGenerated())
                .build();
        columnMap.put(metadataColumn.getSqlName(), metadataColumn);
    }
}
