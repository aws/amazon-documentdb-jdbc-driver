/*
 * Copyright <2021> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.metadata;

import com.google.common.collect.ImmutableSet;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGenerator.combinePath;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGenerator.toName;

class DocumentDbTableSchemaGeneratorTest {
    private static final String COLLECTION_NAME = DocumentDbTableSchemaGeneratorTest.class.getSimpleName();
    private static final boolean DEMO_MODE = false;

    /**
     * Tests a collection where all the fields are consistent.
     */
    @DisplayName("Tests a collection where all the fields are consistent.")
    @Test
    void testCreateScalarSingleDepth() {
        final List<BsonDocument> documentList = new ArrayList<>();
        for (int count = 0; count < 3; count++) {
            // Types not supported in DocumentDB
            //BsonRegularExpression
            //BsonJavaScript
            //BsonJavaScriptWithScope
            //BsonDecimal128
            final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("fieldDecimal128", new BsonDecimal128(Decimal128.POSITIVE_INFINITY))
                    .append("fieldDouble", new BsonDouble(Double.MAX_VALUE))
                    .append("fieldString", new BsonString("新年快乐"))
                    .append("fieldObjectId", new BsonObjectId())
                    .append("fieldBoolean", new BsonBoolean(true))
                    .append("fieldDate", new BsonDateTime(dateTime))
                    .append("fieldInt", new BsonInt32(Integer.MAX_VALUE))
                    .append("fieldLong", new BsonInt64(Long.MAX_VALUE))
                    .append("fieldMaxKey", new BsonMaxKey())
                    .append("fieldMinKey", new BsonMinKey())
                    .append("fieldNull", new BsonNull())
                    .append("fieldBinary", new BsonBinary(new byte[]{0, 1, 2}));
            Assertions.assertTrue(documentList.add(document));
        }

        // Discover the collection metadata.
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator.generate(
                COLLECTION_NAME, documentList.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.size());
        Assertions.assertEquals(COLLECTION_NAME, metadata.get(COLLECTION_NAME).getSqlName());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertEquals(13, metadataTable.getColumnMap().size());
        final Set<JdbcType> integerSet = metadataTable.getColumnMap().values().stream().collect(
                Collectors.groupingBy(DocumentDbSchemaColumn::getSqlType)).keySet();
        Assertions.assertEquals(9, integerSet.size());
        final Set<JdbcType> expectedTypes = ImmutableSet.of(
                JdbcType.BIGINT,
                JdbcType.VARBINARY,
                JdbcType.BOOLEAN,
                JdbcType.DECIMAL,
                JdbcType.DOUBLE,
                JdbcType.INTEGER,
                JdbcType.NULL,
                JdbcType.TIMESTAMP,
                JdbcType.VARBINARY,
                JdbcType.VARCHAR
        );
        Assertions.assertTrue(expectedTypes.containsAll(integerSet));

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * This tests SQL type promotion.
     */
    @DisplayName("This tests SQL type promotion.")
    @Test
    void testSqlTypesPromotion() {
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonType[] supportedBsonTypeSet = new BsonType[]{
                BsonType.BINARY,
                BsonType.BOOLEAN,
                BsonType.DATE_TIME,
                BsonType.DECIMAL128,
                BsonType.DOUBLE,
                BsonType.INT32,
                BsonType.INT64,
                BsonType.MAX_KEY,
                BsonType.MIN_KEY,
                BsonType.NULL,
                BsonType.OBJECT_ID,
                BsonType.STRING,
                BsonType.ARRAY,
                BsonType.DOCUMENT,
        };
        final BsonValue[] supportedBsonValueSet = new BsonValue[]{
                new BsonBinary(new byte[]{0, 1, 2}),
                new BsonBoolean(false),
                new BsonDateTime(dateTime),
                new BsonDecimal128(Decimal128.POSITIVE_INFINITY),
                new BsonDouble(Double.MAX_VALUE),
                new BsonInt32(Integer.MAX_VALUE),
                new BsonInt64(Long.MAX_VALUE),
                new BsonMaxKey(),
                new BsonMinKey(),
                new BsonNull(),
                new BsonObjectId(),
                new BsonString("新年快乐"),
                BsonArray.parse("[ 1, 2, 3 ]"),
                BsonDocument.parse("{ \"field\" : \"value\" }"),
        };

        final List<BsonDocument> documentList = new ArrayList<>();

        for (int outerIndex = 0; outerIndex < supportedBsonTypeSet.length; outerIndex++) {
            final BsonType bsonType = supportedBsonTypeSet[outerIndex];
            final BsonValue bsonValue = supportedBsonValueSet[outerIndex];
            final JdbcType initSqlType = DocumentDbTableSchemaGenerator
                    .getPromotedSqlType(bsonType, JdbcType.NULL);

            final BsonDocument initDocument = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("field", bsonValue);
            for (int innerIndex = 0; innerIndex < supportedBsonTypeSet.length; innerIndex++) {
                documentList.clear();

                final BsonValue nextBsonValue = supportedBsonValueSet[innerIndex];
                final BsonType nextBsonType = supportedBsonTypeSet[innerIndex];
                final JdbcType nextSqlType = DocumentDbTableSchemaGenerator.getPromotedSqlType(
                        nextBsonType, initSqlType);
                final BsonDocument nextDocument = new BsonDocument()
                        .append("_id", new BsonObjectId())
                        .append("field", nextBsonValue);
                documentList.add(initDocument);
                documentList.add(nextDocument);

                // discover the collection metadata
                final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                        .generate(COLLECTION_NAME, documentList.iterator());

                Assertions.assertNotNull(metadata);
                Assertions.assertEquals(producesVirtualTable(bsonType, nextBsonType) ? 2 : 1,
                        metadata.size(), String.format("%s:%s", bsonType, nextBsonType));
                Assertions.assertEquals(COLLECTION_NAME, metadata.get(
                        COLLECTION_NAME).getSqlName());
                final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                        .get(COLLECTION_NAME);
                Assertions.assertEquals(producesVirtualTable(bsonType, nextBsonType) ? 1 : 2,
                        metadataTable.getColumnMap().size(), String.format("%s:%s", bsonType, nextBsonType));
                final DocumentDbSchemaColumn metadataColumn = metadataTable.getColumnMap().get(
                        "field");
                if (!producesVirtualTable(bsonType, nextBsonType)) {
                    Assertions.assertNotNull(metadataColumn);
                    Assertions.assertEquals(nextSqlType, metadataColumn.getSqlType());
                } else {
                    Assertions.assertNull(metadataColumn);
                }

            }
        }
    }

    /**
     * This tests unsupported scalar type promotion.
     */
    @DisplayName("This tests unsupported scalar type promotion.")
    @Test
    void testUnsupportedCreateScalarPromotedSqlTypes() {
        final BsonType[] unsupportedBsonTypeSet = new BsonType[] {
                BsonType.DB_POINTER,
                BsonType.JAVASCRIPT,
                BsonType.JAVASCRIPT_WITH_SCOPE,
                BsonType.REGULAR_EXPRESSION,
                BsonType.SYMBOL,
                BsonType.TIMESTAMP,
                BsonType.UNDEFINED
        };

        // Unsupported types promote to VARCHAR.
        for (final BsonType bsonType : unsupportedBsonTypeSet) {
            Assertions.assertEquals(
                    JdbcType.VARCHAR,
                    DocumentDbTableSchemaGenerator.getPromotedSqlType(bsonType, JdbcType.NULL));
        }
    }

    /**
     * Tests whether all columns are found, even if missing at first.
     */
    @DisplayName("Tests whether all columns are found, even if missing at first.")
    @Test
    void testCreateScalarFieldsMissing() {
        final List<BsonDocument> documentList = new ArrayList<>();
        for (int count = 0; count < 13; count++) {
            final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId());
            if (count == 1) {
                document.append("fieldDouble", new BsonDouble(Double.MAX_VALUE));
            }
            if (count == 2) {
                document.append("fieldString", new BsonString("新年快乐"));
            }
            if (count == 3) {
                document.append("fieldObjectId", new BsonObjectId());
            }
            if (count == 4) {
                document.append("fieldBoolean", new BsonBoolean(true));
            }
            if (count == 5) {
                document.append("fieldDate", new BsonDateTime(dateTime));
            }
            if (count == 6) {
                document.append("fieldInt", new BsonInt32(Integer.MAX_VALUE));
            }
            if (count == 7) {
                document.append("fieldLong", new BsonInt64(Long.MAX_VALUE));
            }
            if (count == 8) {
                document.append("fieldMaxKey", new BsonMaxKey());
            }
            if (count == 9) {
                document.append("fieldMinKey", new BsonMinKey());
            }
            if (count == 10) {
                document.append("fieldNull", new BsonNull());
            }
            if (count == 11) {
                document.append("fieldBinary", new BsonBinary(new byte[]{0, 1, 2}));
            }
            if (count == 12) {
                document.append("fieldDecimal128", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
            }
            Assertions.assertTrue(documentList.add(document));
        }

        // Discover the collection metadata.
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documentList.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.size());
        Assertions.assertEquals(COLLECTION_NAME,
                metadata.get(COLLECTION_NAME).getSqlName());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumnMap().size());
        final Set<JdbcType> integerSet = metadataTable.getColumnMap().values().stream().collect(
                Collectors.groupingBy(DocumentDbSchemaColumn::getSqlType)).keySet();
        Assertions.assertEquals(9, integerSet.size());
        final Set<JdbcType> expectedTypes = ImmutableSet.of(
                JdbcType.BIGINT,
                JdbcType.VARBINARY,
                JdbcType.BOOLEAN,
                JdbcType.DECIMAL,
                JdbcType.DOUBLE,
                JdbcType.INTEGER,
                JdbcType.NULL,
                JdbcType.TIMESTAMP,
                JdbcType.VARBINARY,
                JdbcType.VARCHAR
        );
        Assertions.assertTrue(expectedTypes.containsAll(integerSet));

        final Map<String, String> tableNameMap = new HashMap<>();
        // Check for in-order list of columns.
        int columnIndex = 0;
        for (Entry<String, DocumentDbSchemaColumn> entry : metadataTable.getColumnMap()
                .entrySet()) {
            Assertions.assertTrue(0 != columnIndex
                    || toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap).equals(entry.getKey()));
            Assertions.assertTrue(1 != columnIndex || "fieldDouble".equals(entry.getKey()));
            Assertions.assertTrue(2 != columnIndex || "fieldString".equals(entry.getKey()));
            Assertions.assertTrue(3 != columnIndex || "fieldObjectId".equals(entry.getKey()));
            Assertions.assertTrue(4 != columnIndex || "fieldBoolean".equals(entry.getKey()));
            Assertions.assertTrue(5 != columnIndex || "fieldDate".equals(entry.getKey()));
            Assertions.assertTrue(6 != columnIndex || "fieldInt".equals(entry.getKey()));
            Assertions.assertTrue(7 != columnIndex || "fieldLong".equals(entry.getKey()));
            Assertions.assertTrue(8 != columnIndex || "fieldMaxKey".equals(entry.getKey()));
            Assertions.assertTrue(9 != columnIndex || "fieldMinKey".equals(entry.getKey()));
            Assertions.assertTrue(10 != columnIndex || "fieldNull".equals(entry.getKey()));
            Assertions.assertTrue(11 != columnIndex || "fieldBinary".equals(entry.getKey()));
            Assertions.assertTrue(12 != columnIndex || "fieldDecimal128".equals(entry.getKey()));
            columnIndex++;
        }

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests a two-level document.
     */
    @DisplayName("Tests a two-level document.")
    @Test
    void testComplexTwoLevelDocument() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1 } }");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        final DocumentDbMetadataTable baseTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertNotNull(baseTable);
        Assertions.assertEquals(1, baseTable.getColumnMap().size());
        DocumentDbSchemaColumn schemaColumn = baseTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertEquals(1, schemaColumn.getIndex(baseTable).orElse(null));
        Assertions.assertEquals(1, schemaColumn.getPrimaryKeyIndex(baseTable).orElse(null));
        Assertions.assertNull(schemaColumn.getForeignKeyIndex(baseTable).orElse(null));
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) schemaColumn;
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        final DocumentDbMetadataTable virtualTable = (DocumentDbMetadataTable) metadata
                .get(toName(combinePath(COLLECTION_NAME, "doc"), tableNameMap));
        Assertions.assertEquals(2, virtualTable.getColumnMap().size());

        // _id foreign key column
        schemaColumn = virtualTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertEquals(1, schemaColumn.getIndex(virtualTable).orElse(null));
        Assertions.assertEquals(1, schemaColumn.getPrimaryKeyIndex(virtualTable).orElse(null));
        Assertions.assertEquals(1, schemaColumn.getForeignKeyIndex(baseTable).orElse(null));
        metadataColumn = (DocumentDbMetadataColumn) schemaColumn;
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertEquals(COLLECTION_NAME, metadataColumn.getForeignKeyTableName());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        schemaColumn = virtualTable.getColumnMap().get("field");
        Assertions.assertEquals(2, schemaColumn.getIndex(virtualTable).orElse(null));
        Assertions.assertEquals(0, schemaColumn.getPrimaryKeyIndex(virtualTable).orElse(null));
        Assertions.assertNull(schemaColumn.getForeignKeyIndex(baseTable).orElse(null));
        metadataColumn = (DocumentDbMetadataColumn) schemaColumn;
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertFalse(metadataColumn.isIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }


    /**
     * Tests a three-level document.
     */
    @DisplayName("Tests a three-level document.")
    @Test
    void testComplexThreeLevelDocument() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "doc"), tableNameMap));
        Assertions.assertEquals(2, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertEquals(COLLECTION_NAME, metadataColumn.getForeignKeyTableName());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertFalse(metadataColumn.isIndex());

        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc2"
        final String parentPath = "doc";
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(combinePath(COLLECTION_NAME, parentPath), "doc2"), tableNameMap));
        Assertions.assertEquals(2, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertEquals(COLLECTION_NAME, metadataColumn.getForeignKeyTableName());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field2"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field2", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests a single-level array as virtual table.
     */
    @DisplayName("Tests a single-level array as virtual table.")
    @Test
    void testComplexSingleLevelArray() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ 1, 2, 3 ] }");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertEquals(3, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());
        Assertions.assertEquals(COLLECTION_NAME, metadataColumn.getForeignKeyTableName());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getForeignKeyColumnName());


        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array",
                metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertTrue(metadataColumn.isGenerated());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());

        // value key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals("value", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests a two-level array as virtual table.
     */
    @DisplayName("Tests a two-level array as virtual table.")
    @Test
    void testComplexTwoLevelArray() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ [1, 2, 3 ], [ 4, 5, 6 ] ]}");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertEquals(4, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertEquals(COLLECTION_NAME, metadataColumn.getForeignKeyTableName());
        Assertions.assertEquals(COLLECTION_NAME + "__id", metadataColumn.getForeignKeyColumnName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_1"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_1"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(3, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // value key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals("value", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertNull(metadataColumn.getForeignKeyColumnName());
        Assertions.assertNull(metadataColumn.getForeignKeyTableName());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    @Test
    void testComplexSingleLevelWithDocumentsWithArray() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"array2\" : [ \"a\", \"b\", \"c\" ] } ]}");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for array with name "array"
        metadataTable = (DocumentDbMetadataTable) metadata.get(toName(combinePath(
                COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertEquals(4, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertTrue(metadataColumn.isPrimaryKey());
        Assertions.assertTrue(metadataColumn.isIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // document column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field1"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field1", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertFalse(metadataColumn.isPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());


        // Virtual table for array in array
        metadataTable = (DocumentDbMetadataTable) metadata.get(toName(combinePath(combinePath(
                COLLECTION_NAME, "array"), "array2"), tableNameMap));
        Assertions.assertEquals(4, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(2, metadataColumn.getForeignKeyIndex());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array_array2", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array.array2", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array_array2", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(3, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // value column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "array2"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("value", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests a two-level array as virtual table with multiple documents.
     */
    @DisplayName("Tests a two-level array as virtual table with multiple documents.")
    @Test
    void testComplexSingleLevelArrayWithDocuments() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = (DocumentDbMetadataTable) metadata.get(toName(combinePath(
                COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertEquals(5, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath("array", "index_lvl_0"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // document column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field1"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field1", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field2"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field2", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests a three-level document with multiple documents.
     */
    @DisplayName("Tests a three-level document with multiple documents.")
    @Test
    void testComplexThreeLevelMultipleDocuments() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"doc\" : { \n" +
                        "    \"field\" : 1, \n" +
                        "    \"doc2\" : { \n" +
                        "      \"field1\" : \"value\" \n" +
                        "    } \n" +
                        "} \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"doc\" : { \n" +
                        "    \"field\" : 1, \n" +
                        "    \"doc2\" : { \n" +
                        "      \"field2\" : \"value\" \n" +
                        "    } \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.size());
        DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "doc"), tableNameMap));
        Assertions.assertEquals(2, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc2"
        final String parentPath = "doc";
        metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(combinePath(COLLECTION_NAME, parentPath), "doc2"), tableNameMap));
        Assertions.assertEquals(3, metadataTable.getColumnMap().size());

        // _id foreign key column
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getFieldPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap),
                metadataColumn.getSqlName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field1"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field1", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field2"),
                metadataColumn.getFieldPath());
        Assertions.assertEquals("field2", metadataColumn.getSqlName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
        Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
        Assertions.assertFalse(metadataColumn.isGenerated());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Tests inconsistent data types in arrays should not fail.
     */
    @DisplayName("Tests inconsistent data types in arrays should not fail.")
    @Test
    void testInconsistentArrayDataType() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument[] tests = new BsonDocument[] {
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ 1, [ 2 ] ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ 1, { \"field\" : 2 } ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 2 }, 3 ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 2 }, [ 3, 4 ] ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ [ 1, 2 ], { \"field\" : 2 } ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ [ 1, 2 ], 2 ] }"),
        };

        for (BsonDocument document : tests) {
            final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                    .generate(COLLECTION_NAME, Arrays.stream((new BsonDocument[]{document})).iterator());
            Assertions.assertEquals(2, metadata.size());
            final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                    .get(toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
            Assertions.assertNotNull(metadataTable);
            Assertions.assertEquals(3, metadataTable.getColumnMap().size());

            DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable
                    .getColumnMap().get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(1, metadataColumn.getForeignKeyIndex());
            Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());

            metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                    toName(combinePath("array", "index_lvl_0"), tableNameMap));
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
            Assertions.assertEquals(2, metadataColumn.getPrimaryKeyIndex());

            metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(0, metadataColumn.getForeignKeyIndex());
            Assertions.assertEquals(0, metadataColumn.getPrimaryKeyIndex());
            Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());

            printMetadataOutput(metadata, getMethodName());
        }
    }

    /**
     * Tests that the "_id" field of type DOCUMENT will be promoted to VARCHAR.
     */
    @DisplayName("Tests that the \"_id\" field of type DOCUMENT will be promoted to VARCHAR.")
    @Test
    void testIdFieldIsDocument() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument document = BsonDocument
                .parse("{ \"_id\" : { \"field\" : 1 }, \"field2\" : 2 }");
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singletonList(document).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.size());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumnMap().size());
        final DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKeyIndex());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test whether a conflict is detected in inconsistent arrays over multiple documents.
     * Here, array of object, then array of integer
     */
    @DisplayName("Test whether a conflict is detected in inconsistent arrays over multiple documents. Here, array of object, then array of integer.")
    @Test
    void testMultiDocumentInconsistentArrayDocumentToInt32() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"array\" : [ {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } ] \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"array\" : [ 1, 2, 3 ] \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumnMap().size());
        final DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test whether a conflict is detected in inconsistent arrays over multiple documents.
     * Here, array of array of integer, then array of integer
     */
    @DisplayName("Test whether a conflict is detected in inconsistent arrays over multiple documents. Here, array of array of integer, then array of integer")
    @Test
    void testMultiDocumentInconsistentArrayOfArrayToInt32() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"array\" : [ [ 1, 2 ], [ 3, 4 ] ] \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"array\" : [ 1, 2, 3 ] \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumnMap().size());
        final DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test whether empty sub-documents are handled.
     */
    @DisplayName("Test whether empty sub-documents are handled.")
    @Test
    void testEmptyDocuments() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"doc\" : { } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key2\", \n" +
                        "  \"doc\" : { } \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "doc"), tableNameMap));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumnMap().size());
        final DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test whether null scalars are handled.
     */
    @DisplayName("Test whether null scalars are handled.")
    @Test
    void testNullScalar() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"field\" : null \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key2\", \n" +
                        "  \"field\" : null \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.size());
        final DocumentDbSchemaTable metadataTable = metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.NULL, metadataColumn.getSqlType());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test whether empty array is handled.
     */
    @DisplayName("Test whether empty array is handled.")
    @Test
    void testEmptyArray() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \n" +
                        "  \"array\" : [ ] \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key2\", \n" +
                        "  \"array\" : [ ] \n" +
                        "}"
        );
        documents.add(document);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.size());
        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata.get(
                toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumnMap().size());
        DocumentDbMetadataColumn metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap()
                .get(toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.VARCHAR, metadataColumn.getSqlType());
        metadataColumn = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(JdbcType.NULL, metadataColumn.getSqlType());

        printMetadataOutput(metadata, getMethodName());
    }

    /**
     * Test that primary and foreign key have consistent type after conflict.
     */
    @DisplayName("Test that primary and foreign key have consistent type after conflict.")
    @Test
    void testPrimaryKeyScalarTypeInconsistency() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : 1, \n" +
                        "  \"array\" : [1, 1, 1] \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : 2.1, \n" +
                        "  \"array\" : [ 0.0, 0.0, 0.0] \n" +
                        "}"
        );

        documents.add(document);
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documents.iterator());
        Assertions.assertNotNull(metadata);
        final DocumentDbMetadataTable metadataArrayTable = (DocumentDbMetadataTable) metadata
                .get(toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertNotNull(metadataArrayTable);
        final DocumentDbMetadataColumn metadataColumnArrayId = (DocumentDbMetadataColumn) metadataArrayTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumnArrayId);

        final DocumentDbMetadataTable metadataTable = (DocumentDbMetadataTable) metadata
                .get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        final DocumentDbMetadataColumn metadataColumnId = (DocumentDbMetadataColumn) metadataTable.getColumnMap().get(
                toName(combinePath(COLLECTION_NAME, "_id"), tableNameMap));
        Assertions.assertNotNull(metadataColumnId);

        Assertions.assertEquals(metadataColumnId.getSqlType(), metadataColumnArrayId.getSqlType(),
                "Type of _id columns (DocumentDbTableSchemaGeneratorTest._id and " +
                        "DocumentDbCollectionMetadataTest_array._id) should match");
        Assertions.assertEquals(metadataColumnArrayId.getSqlType(), JdbcType.DOUBLE,
                "Type of ID columns (DocumentDbTableSchemaGeneratorTest._id and " +
                        "DocumentDbCollectionMetadataTest_array._id) should be DOUBLE (" + JdbcType.DOUBLE + ")");
    }

    @Test
    void testDeepStructuredData() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final String json = "{\n"
                + "    \"_id\" : \"60a2c0c65be86c8f6a007514\",\n"
                + "    \"field\" : \"string\",\n"
                + "    \"count\" : 19,\n"
                + "    \"timestamp\" : \"2021-05-17T19:15:18.316Z\",\n"
                + "    \"subDocument\" : {\n"
                + "        \"field\" : \"ABC\",\n"
                + "        \"field2\" : [\n"
                + "            \"A\",\n"
                + "            \"B\",\n"
                + "            \"C\"\n"
                + "        ]\n"
                + "    },\n"
                + "    \"twoLevelArray\" : [\n"
                + "        [\n"
                + "            1,\n"
                + "            2\n"
                + "        ],\n"
                + "        [\n"
                + "            3,\n"
                + "            4\n"
                + "        ],\n"
                + "        [\n"
                + "            5,\n"
                + "            6\n"
                + "        ]\n"
                + "    ],\n"
                + "    \"nestedArray\" : [\n"
                + "        {\n"
                + "            \"document\" : 0,\n"
                + "            \"innerArray\" : [\n"
                + "                1,\n"
                + "                2,\n"
                + "                3\n"
                + "            ]\n"
                + "        },\n"
                + "        {\n"
                + "            \"document\" : 1,\n"
                + "            \"innerArray\" : [\n"
                + "                1,\n"
                + "                2,\n"
                + "                3\n"
                + "            ]\n"
                + "        },\n"
                + "        {\n"
                + "            \"document\" : 2,\n"
                + "            \"innerArray\" : [\n"
                + "                1,\n"
                + "                2,\n"
                + "                3\n"
                + "            ]\n"
                + "        }\n"
                + "    ],\n"
                + "    \"nestedSubDocument\" : {\n"
                + "        \"field\" : 0,\n"
                + "        \"subDoc0\" : {\n"
                + "            \"field\" : 1,\n"
                + "            \"subDoc1\" : {\n"
                + "                \"field\" : 2\n"
                + "            }\n"
                + "        }\n"
                + "    }\n"
                + "}\n";
        final BsonDocument document = BsonDocument.parse(json);

        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singleton(document).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(9, metadata.size());

        final DocumentDbSchemaTable baseTable = metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(baseTable);
        Assertions.assertEquals(4, baseTable.getColumns().size());

        final DocumentDbSchemaTable subDocument = metadata
                .get(toName(combinePath(COLLECTION_NAME, "subDocument"), tableNameMap));
        Assertions.assertNotNull(subDocument);
        Assertions.assertEquals(2, subDocument.getColumns().size());

        final DocumentDbSchemaTable subDocumentField2 = metadata
                .get(toName(combinePath(combinePath(
                        COLLECTION_NAME, "subDocument"), "field2"), tableNameMap));
        Assertions.assertNotNull(subDocumentField2);
        Assertions.assertEquals(3, subDocumentField2.getColumns().size());

        final DocumentDbSchemaTable twoLevelArray = metadata
                .get(toName(combinePath(
                        COLLECTION_NAME, "twoLevelArray"), tableNameMap));
        Assertions.assertNotNull(twoLevelArray);
        Assertions.assertEquals(4, twoLevelArray.getColumns().size());

        final DocumentDbSchemaTable nestedArray = metadata
                .get(toName(combinePath(
                        COLLECTION_NAME, "nestedArray"), tableNameMap));
        Assertions.assertNotNull(nestedArray);
        Assertions.assertEquals(3, nestedArray.getColumns().size());

        final DocumentDbSchemaTable nestedArrayInnerArray = metadata
                .get(toName(combinePath(combinePath(
                        COLLECTION_NAME, "nestedArray"), "innerArray"), tableNameMap));
        Assertions.assertNotNull(nestedArrayInnerArray);
        Assertions.assertEquals(4, nestedArrayInnerArray.getColumns().size());

        final DocumentDbSchemaTable nestedSubDocument = metadata
                .get(toName(combinePath(
                        COLLECTION_NAME, "nestedSubDocument"), tableNameMap));
        Assertions.assertNotNull(nestedSubDocument);
        Assertions.assertEquals(2, nestedSubDocument.getColumns().size());
        final DocumentDbSchemaTable subDoc0 = metadata
                .get(toName(combinePath(combinePath(
                        COLLECTION_NAME, "nestedSubDocument"), "subDoc0"), tableNameMap));
        Assertions.assertNotNull(subDoc0);
        final DocumentDbSchemaTable subDoc1 = metadata
                .get(toName(combinePath(combinePath(combinePath(
                        COLLECTION_NAME, "nestedSubDocument"), "subDoc0"), "subDoc1"), tableNameMap));
        Assertions.assertNotNull(subDoc1);
    }

    @DisplayName("Test whether a sub-document with '_id' as a field is handled correctly.")
    @Test
    void testNestedDocumentWithIdInSubDocument() {
        final String nestedJson = "{\n"
                + "  \"_id\": { \"$oid\": \"607d96b40352ee001f493a73\" },\n"
                + "  \"language\": \"en\",\n"
                + "  \"tags\": [],\n"
                + "  \"title\": \"TT Eval\",\n"
                + "  \"name\": \"tt_eval\",\n"
                + "  \"type\": \"form\",\n"
                + "  \"created\": \"2021-04-19T14:41:56.252Z\",\n"
                + "  \"modified\": \"2021-04-19T14:41:56.252Z\",\n"
                + "  \"owner\": \"12345\",\n"
                + "  \"components\": [\n"
                + "    {\n"
                + "      \"_id\": { \"$oid\": \"607d96b40352ee001f493aca\" },\n"
                + "      \"label\": \"Objective\",\n"
                + "      \"required\": false,\n"
                + "      \"tooltip\": \"additional note go here to describe context of question 54\",\n"
                + "      \"name\": \"tteval-54\",\n"
                + "      \"type\": \"section\",\n"
                + "      \"components\": [\n"
                + "        {\n"
                + "          \"_id\": { \"$oid\": \"607d96b50352ee001f493bdc\" },\n"
                + "          \"label\": \"Strength/ROM\",\n"
                + "          \"required\": false,\n"
                + "          \"tooltip\": \"additional note go here to describe context of question 221\",\n"
                + "          \"name\": \"tteval-221\",\n"
                + "          \"type\": \"section\",\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ],\n"
                + "  \"__v\": { \"$numberInt\": \"0\" }\n"
                + "}\n";
        final BsonDocument document = BsonDocument.parse(nestedJson);
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singleton(document).iterator());
        Assertions.assertNotNull(metadata);
    }

    @DisplayName("Tests when some arrays are empty")
    @Test
    void testEmptyNonEmptyObjectArray() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final String nestedJson = "{\n"
                + "  \"_id\": { \"$oid\": \"607d96b40352ee001f493a73\" },\n"
                + "  \"language\": \"en\",\n"
                + "  \"tags\": [],\n"
                + "  \"title\": \"TT Eval\",\n"
                + "  \"name\": \"tt_eval\",\n"
                + "  \"type\": \"form\",\n"
                + "  \"created\": \"2021-04-19T14:41:56.252Z\",\n"
                + "  \"modified\": \"2021-04-19T14:41:56.252Z\",\n"
                + "  \"owner\": \"12345\",\n"
                + "  \"array\": [\n"
                + "    {\n"
                + "      \"components\": []\n"
                + "    },\n"
                + "    {\n"
                + "      \"components\": [\n"
                + "        {\n"
                + "          \"_id\": { \"$oid\": \"607d96b40352ee001f493acb\" },\n"
                + "          \"label\": \"Objective b\",\n"
                + "          \"required\": false,\n"
                + "          \"tooltip\": \"additional note go here to describe context of question b\",\n"
                + "          \"name\": \"tteval-b\",\n"
                + "          \"type\": \"section\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"components\": []\n"
                + "    }\n"
                + "  ],\n"
                + "  \"__v\": { \"$numberInt\": \"0\" }\n"
                + "}\n";
        final BsonDocument document = BsonDocument.parse(nestedJson);
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singleton(document).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(4, metadata.size());

        DocumentDbSchemaTable schemaTable = metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(9, schemaTable.getColumns().size());

        schemaTable = metadata.get(toName(combinePath(COLLECTION_NAME, "tags"), tableNameMap));
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(3, schemaTable.getColumns().size());
        Assertions
                .assertEquals(JdbcType.NULL, schemaTable.getColumnMap().get("value").getSqlType());

        schemaTable = metadata.get(toName(combinePath(COLLECTION_NAME, "array"), tableNameMap));
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(2, schemaTable.getColumns().size());

        schemaTable = metadata.get(toName(combinePath(combinePath(
                COLLECTION_NAME, "array"), "components"), tableNameMap));
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(9, schemaTable.getColumns().size());
        Assertions
                .assertEquals(JdbcType.VARCHAR, schemaTable.getColumnMap().get("_id").getSqlType());
        Assertions.assertEquals(BsonType.OBJECT_ID,
                schemaTable.getColumnMap().get("_id").getDbType());
        Assertions.assertEquals(JdbcType.BOOLEAN,
                schemaTable.getColumnMap().get("required").getSqlType());
        Assertions.assertEquals(BsonType.BOOLEAN,
                schemaTable.getColumnMap().get("required").getDbType());
        Assertions.assertNull(schemaTable.getColumnMap().get("value"));
    }

    @Test
    @DisplayName("Tests identifier names that are longer than allowed maximum")
    void testLongName() {
        String testPath = "a.b.c";
        final Map<String, String> tableNameMap = new HashMap<>();
        String testName;

        testName = toName(testPath, tableNameMap, 128);
        Assertions.assertEquals("a_b_c", testName);

        testName = toName(testPath, tableNameMap, 4);
        Assertions.assertEquals("a_c", testName);

        // Uses cached value
        testName = toName(testPath, tableNameMap, 128);
        Assertions.assertEquals("a_c", testName);

        testPath = "a.b.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_g", testName);

        testPath = "a.c.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_1", testName);

        testPath = "a.d.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_2", testName);

        testPath = "a.e.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_3", testName);

        testPath = "a.f.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_4", testName);

        testPath = "a.g.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_5", testName);

        testPath = "a.h.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_6", testName);

        testPath = "a.i.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_7", testName);

        testPath = "a.j.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_8", testName);

        testPath = "a.k.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_9", testName);

        testPath = "a.l.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f10", testName);

        testPath = "12345678901.x.y.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("_d_e_f_g", testName);

        testPath = "12345678901";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("1234567890", testName);

        testPath = "12345678901.12345678901";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("2345678901", testName);

        testPath = "12345678903.12345678901";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("2345678902", testName);

        testPath = "12345678901.12345678902";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("2345678903", testName);
    }

    @DisplayName("Tests that even deeply nested documents and array have name length less than max.")
    @Test
    void testDeeplyNestedDocumentsArraysForSqlNameLength() {
        BsonValue doc = new BsonNull();
        for (int i = 999; i >= 0; i--) {
            doc = new BsonDocument("_id", new BsonInt32(i))
                    .append(i + "field", new BsonInt32(i))
                    .append(i + "doc", doc)
                    .append(i + "array", new BsonArray(Collections.singletonList(new BsonInt32(i))));
        }
        final Map<String, DocumentDbSchemaTable> tableMap = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singleton((BsonDocument) doc).iterator());

        Assertions.assertEquals(2000, tableMap.size());
        tableMap.keySet().stream()
                .map(tableName -> tableName.length() <= DEFAULT_IDENTIFIER_MAX_LENGTH)
                .forEach(Assertions::assertTrue);
        tableMap.values().stream()
                .flatMap(schemaTable -> schemaTable.getColumns().stream())
                .map(schemaColumn -> schemaColumn.getSqlName().length()
                        <= DEFAULT_IDENTIFIER_MAX_LENGTH)
                .forEach(Assertions::assertTrue);
    }

    private boolean producesVirtualTable(final BsonType bsonType, final BsonType nextBsonType) {
        return (bsonType == BsonType.ARRAY && nextBsonType == BsonType.ARRAY)
                || (bsonType == BsonType.DOCUMENT && nextBsonType == BsonType.DOCUMENT)
                || (bsonType == BsonType.NULL
                && (nextBsonType == BsonType.ARRAY || nextBsonType == BsonType.DOCUMENT))
                || (nextBsonType == BsonType.NULL
                && (bsonType == BsonType.ARRAY || bsonType == BsonType.DOCUMENT));
    }

    private void printMetadataOutput(final Map<String, DocumentDbSchemaTable> model,
                                     final String testName) {
        if (DEMO_MODE) {
            final String nameOfTest = testName != null ? testName : "TEST";
            System.out.printf("Start of %s%n", nameOfTest);
            System.out.println(model.toString());
            System.out.printf("End of %s%n", nameOfTest);
        }
    }

    private static String getMethodName() {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        final String methodName;
        final int stackDepth = 2;
        if (stackDepth < stackTraceElements.length) {
            methodName = stackTraceElements[stackDepth].getMethodName();
        } else {
            methodName = "";
        }
        return methodName;
    }
}
