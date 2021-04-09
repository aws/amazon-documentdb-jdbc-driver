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
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbCollectionMetadata.combinePath;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbCollectionMetadata.toName;

class DocumentDbCollectionMetadataTest {
    private static final String COLLECTION_NAME = DocumentDbCollectionMetadataTest.class.getSimpleName();
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
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documentList.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.getTables().size());
        Assertions.assertEquals(COLLECTION_NAME,
                metadata.getTables().get(COLLECTION_NAME).getName());
        final Map<String, DocumentDbMetadataTable> metadataTableMap = metadata.getTables();
        final DocumentDbMetadataTable metadataTable = metadataTableMap.get(COLLECTION_NAME);
        Assertions.assertEquals(13, metadataTable.getColumns().size());
        final Set<Integer> integerSet = metadataTable.getColumns().values().stream().collect(
                Collectors.groupingBy(DocumentDbMetadataColumn::getSqlType)).keySet();
        Assertions.assertEquals(9, integerSet.size());
        final Set<Integer> expectedTypes = ImmutableSet.of(
                Types.BIGINT,
                Types.VARBINARY,
                Types.BOOLEAN,
                Types.DECIMAL,
                Types.DOUBLE,
                Types.INTEGER,
                Types.NULL,
                Types.TIMESTAMP,
                Types.VARBINARY,
                Types.VARCHAR
        );
        Assertions.assertTrue(expectedTypes.containsAll(integerSet));

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
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
            final Integer initSqlType = DocumentDbCollectionMetadata.getPromotedSqlType(bsonType, Types.NULL);

            final BsonDocument initDocument = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("field", bsonValue);
            for (int innerIndex = 0; innerIndex < supportedBsonTypeSet.length; innerIndex++) {
                documentList.clear();

                final BsonValue nextBsonValue = supportedBsonValueSet[innerIndex];
                final BsonType nextBsonType = supportedBsonTypeSet[innerIndex];
                final Integer nextSqlType = DocumentDbCollectionMetadata.getPromotedSqlType(
                        nextBsonType, initSqlType);
                final BsonDocument nextDocument = new BsonDocument()
                        .append("_id", new BsonObjectId())
                        .append("field", nextBsonValue);
                documentList.add(initDocument);
                documentList.add(nextDocument);

                // discover the collection metadata
                final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                        COLLECTION_NAME,
                        documentList.iterator());

                Assertions.assertNotNull(metadata);
                Assertions.assertEquals(producesVirtualTable(bsonType, nextBsonType) ? 2 : 1,
                        metadata.getTables().size());
                Assertions.assertEquals(COLLECTION_NAME,metadata.getTables().get(
                        COLLECTION_NAME).getName());
                final Map<String, DocumentDbMetadataTable> metadataTableMap = metadata.getTables();
                final DocumentDbMetadataTable metadataTable = metadataTableMap.get(
                        COLLECTION_NAME);
                Assertions.assertEquals(2, metadataTable.getColumns().size());
                final DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                        "field");
                Assertions.assertNotNull(metadataColumn);
                Assertions.assertEquals(nextSqlType, metadataColumn.getSqlType());
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
                    Types.VARCHAR,
                    DocumentDbCollectionMetadata.getPromotedSqlType(bsonType, Types.NULL));
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
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documentList.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.getTables().size());
        Assertions.assertEquals(COLLECTION_NAME,
                metadata.getTables().get(COLLECTION_NAME).getName());
        final Map<String, DocumentDbMetadataTable> metadataTableMap = metadata.getTables();
        final DocumentDbMetadataTable metadataTable = metadataTableMap.get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumns().size());
        final Set<Integer> integerSet = metadataTable.getColumns().values().stream().collect(
                Collectors.groupingBy(DocumentDbMetadataColumn::getSqlType)).keySet();
        Assertions.assertEquals(9, integerSet.size());
        final Set<Integer> expectedTypes = ImmutableSet.of(
                Types.BIGINT,
                Types.VARBINARY,
                Types.BOOLEAN,
                Types.DECIMAL,
                Types.DOUBLE,
                Types.INTEGER,
                Types.NULL,
                Types.TIMESTAMP,
                Types.VARBINARY,
                Types.VARCHAR
        );
        Assertions.assertTrue(expectedTypes.containsAll(integerSet));

        // Check for in-order list of columns.
        int columnIndex = 0;
        for (Entry<String, DocumentDbMetadataColumn> entry : metadataTable.getColumns()
                .entrySet()) {
            Assertions.assertTrue(0 != columnIndex
                    || toName(combinePath(COLLECTION_NAME, "_id")).equals(entry.getKey()));
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

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests a two-level document.
     */
    @DisplayName("Tests a two-level document.")
    @Test
    void testComplexTwoLevelDocument() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1 } }");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(toName(combinePath(COLLECTION_NAME, "doc")));
        Assertions.assertEquals(2, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getPath());
        Assertions.assertEquals("field", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }


    /**
     * Tests a three-level document.
     */
    @DisplayName("Tests a three-level document.")
    @Test
    void testComplexThreeLevelDocument() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(toName(combinePath(COLLECTION_NAME, "doc")));
        Assertions.assertEquals(3, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getPath());
        Assertions.assertEquals("field", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc2"
        final String parentPath = "doc";
        metadataTable = metadata.getTables().get(
                toName(combinePath(combinePath(COLLECTION_NAME, parentPath), "doc2")));
        Assertions.assertEquals(2, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field2"),
                metadataColumn.getPath());
        Assertions.assertEquals("field2", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests a single-level array as virtual table.
     */
    @DisplayName("Tests a single-level array as virtual table.")
    @Test
    void testComplexSingleLevelArray() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ 1, 2, 3 ] }");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "array")));
        Assertions.assertEquals(3, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // value key column
        metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getPath());
        Assertions.assertEquals("value", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests a two-level array as virtual table.
     */
    @DisplayName("Tests a two-level array as virtual table.")
    @Test
    void testComplexTwoLevelArray() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ [1, 2, 3 ], [ 4, 5, 6 ] ]}");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(toName(combinePath(
                COLLECTION_NAME, "array")));
        Assertions.assertEquals(4, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_1")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_1")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_1")),
                metadataColumn.getName());
        Assertions.assertEquals(3, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // value key column
        metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals("array", metadataColumn.getPath());
        Assertions.assertEquals("value", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test metadata creation for document with array in document in array.
     */
    @DisplayName("Test metadata creation for document with array in document in array.")
    @Test
    void testComplexSingleLevelWithDocumentsWithArray() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"array2\" : [ \"a\", \"b\", \"c\" ] } ]}");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns()
                .get(toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for array with name "array"
        metadataTable = metadata.getTables().get(toName(combinePath(
                COLLECTION_NAME, "array")));
        Assertions.assertEquals(5, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // document column
        metadataColumn = metadataTable.getColumns().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field"),
                metadataColumn.getPath());
        Assertions.assertEquals("field", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = metadataTable.getColumns().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field1"),
                metadataColumn.getPath());
        Assertions.assertEquals("field1", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());


        // Virtual table for array in array
        metadataTable = metadata.getTables().get(toName(combinePath(combinePath(
                COLLECTION_NAME, "array"), "array2")));
        Assertions.assertEquals(4, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(2, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array_array2", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array_array2", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array_array2", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(3, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // value column
        metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "array2"),
                metadataColumn.getPath());
        Assertions.assertEquals("value", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests a two-level array as virtual table with multiple documents.
     */
    @DisplayName("Tests a two-level array as virtual table with multiple documents.")
    @Test
    void testComplexSingleLevelArrayWithDocuments() {
        final BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME,
                Arrays.stream((new BsonDocument[]{document})).iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns()
                .get(toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(toName(combinePath(
                COLLECTION_NAME, "array")));
        Assertions.assertEquals(5, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id",metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // index key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath("array", "index_lvl_0")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.BIGINT, metadataColumn.getSqlType());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath("array", "index_lvl_0")),
                metadataColumn.getName());
        Assertions.assertEquals(2, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertTrue(metadataColumn.isGenerated());

        // document column
        metadataColumn = metadataTable.getColumns().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field"),
                metadataColumn.getPath());
        Assertions.assertEquals("field", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = metadataTable.getColumns().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field1"),
                metadataColumn.getPath());
        Assertions.assertEquals("field1", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // document column
        metadataColumn = metadataTable.getColumns().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("array", "field2"),
                metadataColumn.getPath());
        Assertions.assertEquals("field2", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests a three-level document with multiple documents.
     */
    @DisplayName("Tests a three-level document with multiple documents.")
    @Test
    void testComplexThreeLevelMultipleDocuments() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(3, metadata.getTables().size());
        DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc"
        metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "doc")));
        Assertions.assertEquals(3, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.INTEGER, metadataColumn.getSqlType());
        Assertions.assertEquals(combinePath("doc", "field"),
                metadataColumn.getPath());
        Assertions.assertEquals("field", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        // Virtual table for document with name "doc2"
        final String parentPath = "doc";
        metadataTable = metadata.getTables().get(
                toName(combinePath(combinePath(COLLECTION_NAME, parentPath), "doc2")));
        Assertions.assertEquals(3, metadataTable.getColumns().size());

        // _id foreign key column
        metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals("_id", metadataColumn.getPath());
        Assertions.assertEquals(
                toName(combinePath(COLLECTION_NAME, "_id")),
                metadataColumn.getName());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(1, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field1");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field1"),
                metadataColumn.getPath());
        Assertions.assertEquals("field1", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        metadataColumn = metadataTable.getColumns().get("field2");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(
                combinePath(combinePath(parentPath, "doc2"), "field2"),
                metadataColumn.getPath());
        Assertions.assertEquals("field2", metadataColumn.getName());
        Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
        Assertions.assertEquals(0, metadataColumn.getForeignKey());
        Assertions.assertFalse(metadataColumn.isGenerated());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Tests inconsistent data types in arrays should not fail.
     */
    @DisplayName("Tests inconsistent data types in arrays should not fail.")
    @Test
    void testInconsistentArrayDataType() {
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
            final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata
                    .create(COLLECTION_NAME,
                            Arrays.stream((new BsonDocument[]{document})).iterator());
            Assertions.assertEquals(2, metadata.getTables().size());
            final DocumentDbMetadataTable metadataTable = metadata.getTables()
                    .get(toName(combinePath(COLLECTION_NAME, "array")));
            Assertions.assertNotNull(metadataTable);
            Assertions.assertEquals(3, metadataTable.getColumns().size());

            DocumentDbMetadataColumn metadataColumn = metadataTable
                    .getColumns().get(toName(combinePath(COLLECTION_NAME, "_id")));
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(1, metadataColumn.getForeignKey());
            Assertions.assertEquals(1, metadataColumn.getPrimaryKey());

            metadataColumn = metadataTable.getColumns().get(
                    toName(combinePath("array", "index_lvl_0")));
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(0, metadataColumn.getForeignKey());
            Assertions.assertEquals(2, metadataColumn.getPrimaryKey());

            metadataColumn = metadataTable.getColumns().get("value");
            Assertions.assertNotNull(metadataColumn);
            Assertions.assertEquals(0, metadataColumn.getForeignKey());
            Assertions.assertEquals(0, metadataColumn.getPrimaryKey());
            Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());

            final String methodName = new Object() {
            }.getClass().getEnclosingMethod().getName();
            printMetadataOutput(metadata, methodName);
        }
    }

    /**
     * Tests that the "_id" field of type DOCUMENT will be promoted to VARCHAR.
     */
    @DisplayName("Tests that the \"_id\" field of type DOCUMENT will be promoted to VARCHAR.")
    @Test
    void testIdFieldIsDocument() {
        final BsonDocument document = BsonDocument
                .parse("{ \"_id\" : { \"field\" : 1 }, \"field2\" : 2 }");
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, Arrays.asList(document).iterator());
        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        final DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        Assertions.assertEquals(1, metadataColumn.getPrimaryKey());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test whether a conflict is detected in inconsistent arrays over multiple documents.
     * Here, array of object, then array of integer
     */
    @DisplayName("Test whether a conflict is detected in inconsistent arrays over multiple documents. Here, array of object, then array of integer.")
    @Test
    void testMultiDocumentInconsistentArrayDocumentToInt32() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "array")));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumns().size());
        final DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test whether a conflict is detected in inconsistent arrays over multiple documents.
     * Here, array of array of integer, then array of integer
     */
    @DisplayName("Test whether a conflict is detected in inconsistent arrays over multiple documents. Here, array of array of integer, then array of integer")
    @Test
    void testMultiDocumentInconsistentArrayOfArrayToInt32() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "array")));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumns().size());
        final DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test whether empty sub-documents are handled.
     */
    @DisplayName("Test whether empty sub-documents are handled.")
    @Test
    void testEmptyDocuments() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "doc")));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(1, metadataTable.getColumns().size());
        final DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns()
                .get(toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test whether null scalars are handled.
     */
    @DisplayName("Test whether null scalars are handled.")
    @Test
    void testNullScalar() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(1, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(
                COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(2, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns()
                .get(toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        metadataColumn = metadataTable.getColumns()
                .get("field");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.NULL, metadataColumn.getSqlType());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test whether empty array is handled.
     */
    @DisplayName("Test whether empty array is handled.")
    @Test
    void testEmptyArray() {
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

        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());

        Assertions.assertNotNull(metadata);
        Assertions.assertEquals(2, metadata.getTables().size());
        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "array")));
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(3, metadataTable.getColumns().size());
        DocumentDbMetadataColumn metadataColumn = metadataTable.getColumns()
                .get(toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.VARCHAR, metadataColumn.getSqlType());
        metadataColumn = metadataTable.getColumns().get("value");
        Assertions.assertNotNull(metadataColumn);
        Assertions.assertEquals(Types.NULL, metadataColumn.getSqlType());

        final String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        printMetadataOutput(metadata, methodName);
    }

    /**
     * Test that primary and foreign key have consistent type after conflict.
     */
    @DisplayName("Test that primary and foreign key have consistent type after conflict.")
    @Test
    void testPrimaryKeyScalarTypeInconsistency() {
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
        final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata.create(
                COLLECTION_NAME, documents.iterator());
        Assertions.assertNotNull(metadata);
        final DocumentDbMetadataTable metadataArrayTable = metadata.getTables().get(
                toName(combinePath(COLLECTION_NAME, "array")));
        Assertions.assertNotNull(metadataArrayTable);
        final DocumentDbMetadataColumn metadataColumnArrayId = metadataArrayTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumnArrayId);

        final DocumentDbMetadataTable metadataTable = metadata.getTables().get(COLLECTION_NAME);
        Assertions.assertNotNull(metadataTable);
        final DocumentDbMetadataColumn metadataColumnId = metadataTable.getColumns().get(
                toName(combinePath(COLLECTION_NAME, "_id")));
        Assertions.assertNotNull(metadataColumnId);

        Assertions.assertEquals(metadataColumnId.getSqlType(), metadataColumnArrayId.getSqlType(),
                "Type of _id columns (DocumentDbCollectionMetadataTest._id and " +
                        "DocumentDbCollectionMetadataTest_array._id) should match");
        Assertions.assertEquals(metadataColumnArrayId.getSqlType(), Types.DOUBLE,
                "Type of ID columns (DocumentDbCollectionMetadataTest._id and " +
                        "DocumentDbCollectionMetadataTest_array._id) should be DOUBLE (" + Types.DOUBLE + ")");
    }

    private boolean producesVirtualTable(final BsonType bsonType, final BsonType nextBsonType) {
        return (bsonType == BsonType.ARRAY && nextBsonType == BsonType.ARRAY)
                || (bsonType == BsonType.DOCUMENT && nextBsonType == BsonType.DOCUMENT)
                || (bsonType == BsonType.NULL
                && (nextBsonType == BsonType.ARRAY || nextBsonType == BsonType.DOCUMENT));
    }

    private void printMetadataOutput(final DocumentDbCollectionMetadata model,
            final String testName) {
        if (DEMO_MODE) {
            final String nameOfTest = testName != null ? testName : "TEST";
            System.out.printf("Start of %s%n", nameOfTest);
            System.out.println(model.toString());
            System.out.printf("End of %s%n", nameOfTest);
        }
    }
}
