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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.sql.Types;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents the fields in a collection and their data types. A collection can be broken up into
 * one (the base table) or more tables (virtual tables from embedded documents or arrays).
 */
@Getter
public class DocumentDbCollectionMetadata extends DocumentDbSchemaCollection {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbCollectionMetadata.class);
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_EMPTY)
            .enable(SerializationFeature.INDENT_OUTPUT);
    private static final String EMPTY_STRING = "";
    private static final String INDEX_COLUMN_NAME_PREFIX = "index_lvl_";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final String PATH_SEPARATOR = ".";
    private static final String ID_FIELD_NAME = "_id";
    private static final int ID_PRIMARY_KEY_COLUMN = 1;
    private static final int KEY_COLUMN_NONE = 0;

    /**
     * The map of data type promotions.
     *
     * @see <a href="https://docs.mongodb.com/bi-connector/current/schema/type-conflicts#scalar-scalar-conflicts">
     * Map Relational Schemas to MongoDB - Scalar-Scalar Conflicts</a>
     */
    private static final ImmutableMap<Entry<Integer, BsonType>, Integer> PROMOTION_MAP =
            new ImmutableMap.Builder<Entry<Integer, BsonType>, Integer>()
                    .put(new SimpleEntry<>(Types.NULL, BsonType.BOOLEAN), Types.BOOLEAN)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.DATE_TIME), Types.TIMESTAMP)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.DOUBLE), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.INT32), Types.INTEGER)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.INT64), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.NULL), Types.NULL)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.ARRAY), Types.ARRAY)
                    .put(new SimpleEntry<>(Types.NULL, BsonType.DOCUMENT), Types.JAVA_OBJECT)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.BOOLEAN), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.DECIMAL128), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.DOUBLE), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.INT32), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.INT64), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.NULL), Types.ARRAY)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.ARRAY), Types.ARRAY)
                    .put(new SimpleEntry<>(Types.ARRAY, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.BOOLEAN), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.DECIMAL128), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.DOUBLE), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.INT32), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.INT64), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.NULL), Types.JAVA_OBJECT)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.JAVA_OBJECT, BsonType.DOCUMENT), Types.JAVA_OBJECT)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.BOOLEAN), Types.BOOLEAN)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.DOUBLE), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.INT32), Types.INTEGER)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.INT64), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.NULL), Types.BOOLEAN)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BOOLEAN, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.BOOLEAN), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.DOUBLE), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.INT32), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.INT64), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.NULL), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.BIGINT, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.BOOLEAN), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.DOUBLE), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.INT32), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.INT64), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.NULL), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DECIMAL, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.BOOLEAN), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.DOUBLE), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.INT32), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.INT64), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.NULL), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.DOUBLE, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.BOOLEAN), Types.INTEGER)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.DECIMAL128), Types.DECIMAL)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.DOUBLE), Types.DOUBLE)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.INT32), Types.INTEGER)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.INT64), Types.BIGINT)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.NULL), Types.INTEGER)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.INTEGER, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.BOOLEAN), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.DATE_TIME), Types.TIMESTAMP)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.DECIMAL128), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.DOUBLE), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.INT32), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.INT64), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.NULL), Types.TIMESTAMP)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.TIMESTAMP, BsonType.DOCUMENT), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.BOOLEAN), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.DATE_TIME), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.DECIMAL128), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.DOUBLE), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.INT32), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.INT64), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.MAX_KEY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.MIN_KEY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.NULL), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.OBJECT_ID), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.STRING), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.ARRAY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARBINARY, BsonType.DOCUMENT), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.BOOLEAN), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.BINARY), Types.VARBINARY)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.DATE_TIME), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.DECIMAL128), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.DOUBLE), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.INT32), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.INT64), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.MAX_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.MIN_KEY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.NULL), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.OBJECT_ID), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.STRING), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.ARRAY), Types.VARCHAR)
                    .put(new SimpleEntry<>(Types.VARCHAR, BsonType.DOCUMENT), Types.VARCHAR)
                    .build();

    /**
     * All args constructor for collection metadata.
     *
     * @param name       Name of the collection.
     * @param version    Version of this metadata.
     * @param tables     Tables contained within this collection, indexed by name. Uses LinkedHashMap to preserve order.
     */
    public DocumentDbCollectionMetadata(final String name,
                                        final int version,
                                        final LinkedHashMap<String, DocumentDbSchemaTable> tables) {
        super(name, version, tables);
    }

    @Override
    public String toString() {
        try {
            return JSON_OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            DocumentDbCollectionMetadata.LOGGER.error("Error converting object to JSON.", e);
        }
        return DocumentDbCollectionMetadata.EMPTY_STRING;
    }

    /**
     * Creates new collection metadata for a given collection from the provided data.
     *
     * @param collectionName the name of the collection this model should refer.
     * @param cursor         the cursor for the data from which to create a model.
     * @return a new {@link DocumentDbCollectionMetadata} built from the data.
     */
    public static DocumentDbSchemaCollection create(
            final String databaseName,
            final String collectionName,
            final Iterator<BsonDocument> cursor) {
        final LinkedHashMap<String, DocumentDbSchemaTable> tableMap = new LinkedHashMap<>();
        while (cursor.hasNext()) {
            final BsonDocument document = cursor.next();
            processDocument(document, tableMap, new ArrayList<>(),
                    EMPTY_STRING, collectionName, true);
        }

        return new DocumentDbSchemaCollection(databaseName, 1, tableMap);
    }

    /**
     * Process a document including fields, sub-documents and arrays.
     *
     * @param document    the document to process.
     * @param tableMap    the map of virtual tables
     * @param foreignKeys the list of foreign keys.
     * @param path        the path for this field.
     */
    static void processDocument(
            final BsonDocument document,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final String collectionName,
            final boolean isRootDocument) {

        // Need to preserve order of fields.
        final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap = new LinkedHashMap<>();

        final String tableName = toName(combinePath(collectionName, path));
        if (tableMap.containsKey(tableName)) {
            // If we've already visited this document/table,
            // start with the previously discovered columns.
            // This will have included and primary/foreign key definitions.
            columnMap.putAll(tableMap.get(tableName).getColumns());
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

        // Process all fields in the document
        for (Entry<String, BsonValue> entry : document.entrySet()) {
            final String fieldName = entry.getKey();
            final String fieldPath = combinePath(path, fieldName);
            final BsonValue bsonValue = entry.getValue();
            final BsonType bsonType = bsonValue.getBsonType();
            final boolean isPrimaryKey = isRootDocument && isIdField(fieldName);
            final String columnName = getFieldNameIfIsPrimaryKey(
                    collectionName, fieldName, isPrimaryKey);
            final DocumentDbMetadataColumn prevMetadataColumn = (DocumentDbMetadataColumn) columnMap.getOrDefault(
                    columnName, null);

            // ASSUMPTION: relying on the behaviour that the "_id" field will ALWAYS be first
            // in the root document.
            final int prevSqlType = getPrevSqlTypeOrDefault(prevMetadataColumn);
            final int nextSqlType = getSqlTypeIfIsPrimaryKey(bsonType, prevSqlType, isPrimaryKey);

            processComplexTypes(tableMap, foreignKeys, collectionName, entry, fieldPath, bsonType, prevMetadataColumn, nextSqlType);
            final DocumentDbMetadataColumn metadataColumn = DocumentDbMetadataColumn
                    .builder()
                    .fieldPath(fieldPath)
                    .sqlName(columnName)
                    .sqlType(nextSqlType)
                    .dbType(bsonType.getValue())
                    .isIndex(false)
                    .isPrimaryKey(isPrimaryKey)
                    .index(getPrevIndexOrDefault(prevMetadataColumn, columnMap.size() + 1))
                    .tableName(tableName)
                    .primaryKeyIndex(getPrimaryKeyColumn(isPrimaryKey))
                    .foreignKeyIndex(KEY_COLUMN_NONE)
                    .isGenerated(false)
                    .virtualTableName(getVirtualTableNameIfIsPrimaryKey(
                            fieldPath, nextSqlType, isPrimaryKey, collectionName))
                    .build();
            columnMap.put(metadataColumn.getSqlName(), metadataColumn);
            addToForeignKeysIfIsPrimary(foreignKeys, isPrimaryKey, metadataColumn);
        }

        // Ensure virtual table primary key column data types are consistent.
        if (isRootDocument) {
            checkVirtualTablePrimaryKeys(tableMap, path, columnMap);
        }
        // Add virtual table.
        final DocumentDbMetadataTable metadataTable = DocumentDbMetadataTable
                .builder()
                .sqlName(tableName)
                .collectionName(collectionName)
                .columns(columnMap)
                .build();
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
     */
    private static void processArray(
            final BsonArray array,
            final Map<String, DocumentDbSchemaTable> tableMap,
            final List<DocumentDbMetadataColumn> foreignKeys,
            final String path,
            final int arrayLevel,
            final String collectionName) {

        // Need to preserve order of fields.
        final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap = new LinkedHashMap<>();

        int primaryKeyColumn = KEY_COLUMN_NONE;
        int level = arrayLevel;
        DocumentDbMetadataColumn metadataColumn;

        int prevSqlType = Types.NULL;
        int sqlType;
        final String tableName = toName(combinePath(collectionName, path));

        if (tableMap.containsKey(tableName)) {
            // If we've already visited this document/table,
            // start with the previously discovered columns.
            // This will have included and primary/foreign key definitions.
            columnMap.putAll(tableMap.get(tableName).getColumns());
            final String valueColumnPath = VALUE_COLUMN_NAME;
            // TODO: Figure out if previous type was array of array.
            if (columnMap.containsKey(toName(valueColumnPath))) {
                prevSqlType = columnMap.get(toName(valueColumnPath)).getSqlType();
            } else {
                prevSqlType = Types.JAVA_OBJECT;
            }
        }

        // Find the promoted SQL data type for all elements.
        sqlType = prevSqlType;
        for (BsonValue element : array) {
            sqlType = getPromotedSqlType(element.getBsonType(), sqlType);
        }
        if (!isComplexType(sqlType)) {
            if (isComplexType(prevSqlType)) {
                // If promoted to scalar type from complex type, remove previous definition.
                handleComplexToScalarConflict(tableMap, tableName, columnMap);
            } else {
                // Check to see if we're processing scalars at a different level than previously
                // detected.
                sqlType = handleArrayLevelConflict(columnMap, level, sqlType);
            }
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

            final String indexColumnName = toName(combinePath(path, INDEX_COLUMN_NAME_PREFIX + level));
            if (!columnMap.containsKey(toName(indexColumnName))) {
                // Add index column. Although it has no path in the original document, we will
                // use the path of the generated index field once the original document is unwound.
                primaryKeyColumn++;
                metadataColumn = DocumentDbMetadataColumn
                        .builder()
                        .sqlName(toName(indexColumnName))
                        .fieldPath(path)  // Once unwound, the index will be at root level so path = name.
                        .sqlType(Types.BIGINT)
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
            case Types.JAVA_OBJECT:
                processDocumentsInArray(array,
                        tableMap,
                        foreignKeys,
                        path,
                        collectionName);
                break;
            case Types.ARRAY:
                // This will add another level to the array.
                level++;
                processArrayInArray(array,
                        tableMap,
                        foreignKeys,
                        path,
                        collectionName,
                        level);
                break;
            default:
                processValuesInArray(
                        tableMap,
                        path,
                        collectionName,
                        columnMap,
                        sqlType);
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
     */
    private static void processValuesInArray(
            final Map<String, DocumentDbSchemaTable> tableMap,
            final String path,
            final String collectionName,
            final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap,
            final int sqlType) {

        final String tableName = toName(combinePath(collectionName, path));
        // Get column if it already exists so we can preserve index order.
        final DocumentDbMetadataColumn prevMetadataColumn = (DocumentDbMetadataColumn) columnMap.get(toName(VALUE_COLUMN_NAME));
        // Add value column
        final DocumentDbMetadataColumn metadataColumn = DocumentDbMetadataColumn
                .builder()
                .fieldPath(path)
                .sqlName(toName(VALUE_COLUMN_NAME))
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
     */
    private static void processArrayInArray(final BsonArray array,
                                            final Map<String, DocumentDbSchemaTable> tableMap,
                                            final List<DocumentDbMetadataColumn> foreignKeys,
                                            final String path,
                                            final String collectionName,
                                            final int level) {

        for (BsonValue element : array) {
            processArray(element.asArray(),
                    tableMap, foreignKeys, path, level, collectionName);
        }
    }

    /**
     * Processes document elements in an array.
     *
     * @param array          the array elements to scan.
     * @param tableMap       the table map of virtual tables.
     * @param foreignKeys    the list of foreign keys.
     * @param path           the path to this array
     * @param collectionName the name of the collection.
     *                       encountered.
     */
    private static void processDocumentsInArray(final BsonArray array,
                                                final Map<String, DocumentDbSchemaTable> tableMap,
                                                final List<DocumentDbMetadataColumn> foreignKeys,
                                                final String path,
                                                final String collectionName) {

        // This will make the document fields part of this array.
        for (BsonValue element : array) {
            processDocument(element.asDocument(),
                    tableMap, foreignKeys, path, collectionName, false);
        }
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

    /**
     * Combines two paths to form a new path.
     *
     * @param path      the root/parent path.
     * @param fieldName the field name to append to the path.
     * @return a new path with the fieldName append to the root path separated by a period.
     */
    public static String combinePath(final String path, final String fieldName) {
        final String pathSeparator = !DocumentDbConnectionProperties.isNullOrWhitespace(path) &&
                !DocumentDbConnectionProperties.isNullOrWhitespace(fieldName)
                ? PATH_SEPARATOR : EMPTY_STRING;
        final String newPath = !DocumentDbConnectionProperties.isNullOrWhitespace(path)
                ? path : EMPTY_STRING;
        final String newFieldName = !DocumentDbConnectionProperties.isNullOrWhitespace(fieldName)
                ? fieldName : EMPTY_STRING;

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
    static Integer getPromotedSqlType(final BsonType bsonType, final int prevSqlType) {
        final Entry<Integer, BsonType> key = new SimpleEntry<>(prevSqlType, bsonType);
        return PROMOTION_MAP.getOrDefault(key, Types.VARCHAR);
    }

    /**
     * Gets whether the field is the "_id" field.
     *
     * @param fieldName the name of the field.
     * @return returns {@code true} if the field name if "_id", {@code false} otherwise.
     */
    private static boolean isIdField(final String fieldName) {
        return ID_FIELD_NAME.equals(fieldName);
    }

    /**
     * Converts the path to name swapping the period character for an underscore character.
     *
     * @param path the path to convert.
     * @return a string with the period character swapped for an underscore character.
     */
    @VisibleForTesting
    static String toName(final String path) {
        return path.replaceAll("\\.", "_");
    }

    /**
     * Handles a complex to scalar conflict by removing the previous table map and clearing
     * existing column map.
     *
     * @param tableMap  the table map.
     * @param path      the path to the table.
     * @param columnMap the column map.
     */
    private static void handleComplexToScalarConflict(
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
    private static int handleArrayLevelConflict(
            final Map<String, DocumentDbSchemaColumn> columnMap,
            final int level,
            final int sqlType) {

        int newSqlType = sqlType;

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

    private static int getPrimaryKeyColumn(final boolean isPrimaryKey) {
        // If primary key, then first column, zero indicates not part of primary key.
        return isPrimaryKey ? ID_PRIMARY_KEY_COLUMN : KEY_COLUMN_NONE;
    }

    private static String getFieldNameIfIsPrimaryKey(final String path, final String fieldName,
                                                     final boolean isPrimaryKey) {
        return isPrimaryKey
                // For the primary key, qualify it with the parent name.
                ? toName(combinePath(getParentName(path), fieldName))
                : fieldName;
    }

    private static String getVirtualTableNameIfIsPrimaryKey(
            final String fieldPath,
            final int nextSqlType,
            final boolean isPrimaryKey,
            final String collectionName) {
        return !isPrimaryKey && (nextSqlType == Types.ARRAY || nextSqlType == Types.JAVA_OBJECT)
                ? toName(combinePath(collectionName, fieldPath))
                : null;
    }

    private static int getSqlTypeIfIsPrimaryKey(
            final BsonType bsonType,
            final int prevSqlType,
            final boolean isPrimaryKey) {
        return isPrimaryKey && bsonType == BsonType.DOCUMENT
                ? Types.VARCHAR
                : getPromotedSqlType(bsonType, prevSqlType);
    }

    private static int getPrevSqlTypeOrDefault(final DocumentDbMetadataColumn prevMetadataColumn) {
        return prevMetadataColumn != null
                ? prevMetadataColumn.getSqlType()
                : Types.NULL;
    }

    private static boolean isComplexType(final int sqlType) {
        return sqlType == Types.JAVA_OBJECT || sqlType == Types.ARRAY;
    }

    private static void addToForeignKeysIfIsPrimary(
            final List<DocumentDbMetadataColumn> foreignKeys,
            final boolean isPrimaryKey,
            final DocumentDbMetadataColumn metadataColumn) {
        // Add the key to the foreign keys for child tables.
        if (isPrimaryKey) {
            foreignKeys.add(metadataColumn);
        }
    }

    private static int getPrevIndexOrDefault(final DocumentDbMetadataColumn prevMetadataColumn,
                                             final int defaultValue) {
        return prevMetadataColumn != null
                ? prevMetadataColumn.getIndex()
                : defaultValue;
    }

    private static void processComplexTypes(final Map<String, DocumentDbSchemaTable> tableMap,
                                            final List<DocumentDbMetadataColumn> foreignKeys,
                                            final String collectionName,
                                            final Entry<String, BsonValue> entry,
                                            final String fieldPath,
                                            final BsonType bsonType,
                                            final DocumentDbMetadataColumn prevMetadataColumn,
                                            final int nextSqlType) {
        if (nextSqlType == Types.JAVA_OBJECT && bsonType != BsonType.NULL) {
            // This will create/update virtual table.
            processDocument(entry.getValue().asDocument(),
                    tableMap, foreignKeys, fieldPath, collectionName, false);

        } else if (nextSqlType == Types.ARRAY && bsonType != BsonType.NULL) {
            // This will create/update virtual table.
            processArray(entry.getValue().asArray(),
                    tableMap, foreignKeys, fieldPath, 0, collectionName);
        } else {
            // Process a scalar data type.
            if (prevMetadataColumn != null && prevMetadataColumn.getVirtualTableName() != null) {
                // This column has been promoted to a scalar type from a complex type.
                // Remove the previously defined virtual table.
                tableMap.remove(prevMetadataColumn.getVirtualTableName());
            }
        }
    }

    private static void checkVirtualTablePrimaryKeys(final Map<String, DocumentDbSchemaTable> tableMap,
                                                     final String path,
                                                     final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap) {
        final String primaryKeyColumnName = toName(combinePath(path, ID_FIELD_NAME));
        final DocumentDbMetadataColumn primaryKeyColumn = (DocumentDbMetadataColumn) columnMap.get(primaryKeyColumnName);
        for (DocumentDbSchemaTable table : tableMap.values()) {
            final DocumentDbMetadataColumn column = (DocumentDbMetadataColumn) table.getColumns().get(primaryKeyColumnName);
            if (column != null) {
                column.setSqlType(primaryKeyColumn.getSqlType());
            }
        }
    }

    private static void buildForeignKeysFromDocument(final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap,
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
