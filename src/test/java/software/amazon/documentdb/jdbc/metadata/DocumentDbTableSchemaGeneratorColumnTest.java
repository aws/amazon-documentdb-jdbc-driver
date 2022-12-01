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

import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.combinePath;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.getPromotedSqlType;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.toName;

/**
 * These tests check whether columns are generated with the correct type given type conflicts, unsupported types,
 * and instances of null or missing.
 */
public class DocumentDbTableSchemaGeneratorColumnTest extends DocumentDbTableSchemaGeneratorTest {
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
                    .append("fieldDecimal128", new BsonDecimal128(Decimal128.parse(String.valueOf(Double.MAX_VALUE))))
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
                new BsonDecimal128(Decimal128.parse(String.valueOf(Double.MAX_VALUE))),
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
            final JdbcType initSqlType = getPromotedSqlType(bsonType, JdbcType.NULL);

            final BsonDocument initDocument = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("field", bsonValue);
            for (int innerIndex = 0; innerIndex < supportedBsonTypeSet.length; innerIndex++) {
                documentList.clear();

                final BsonValue nextBsonValue = supportedBsonValueSet[innerIndex];
                final BsonType nextBsonType = supportedBsonTypeSet[innerIndex];
                final JdbcType nextSqlType = getPromotedSqlType(
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
                BsonType.UNDEFINED
        };

        // Unsupported types promote to VARCHAR.
        for (final BsonType bsonType : unsupportedBsonTypeSet) {
            Assertions.assertEquals(
                    JdbcType.VARCHAR,
                    getPromotedSqlType(bsonType, JdbcType.NULL));
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
                document.append("fieldDecimal128", new BsonDecimal128(Decimal128.parse(String.valueOf(Double.MAX_VALUE))));
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
     * Tests inconsistent data types in arrays should not fail.
     */
    @DisplayName("Tests inconsistent data types in arrays should not fail.")
    @Test
    void testInconsistentArrayDataType() {
        final Map<String, String> tableNameMap = new HashMap<>();
        final BsonDocument[] tests = new BsonDocument[] {
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ 1, [ 2 ], null ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ 1, { \"field\" : 2 } ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 2 }, 3 ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 2 }, [ 3, 4 ] ] }"),
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ [ 1, 2 ], { \"field\" : 2 }, null ] }"),
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
}
