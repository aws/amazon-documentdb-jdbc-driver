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

package software.amazon.documentdb.jdbc.persist;

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
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;
import software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static software.amazon.documentdb.jdbc.persist.FileSchemaReader.DEFAULT_SCHEMA_NAME;


class FileSchemaReaderTest {
    private static  final String DATABASE_NAME = "testDb";
    private static final String COLLECTION_NAME = FileSchemaReaderTest.class.getSimpleName();
    private static String tableId;

    @BeforeAll
    static void beforeAll() {
        final List<BsonDocument> documentList = new ArrayList<>();
        for (int count = 0; count < 3; count++) {
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
        final DocumentDbSchema schema = new DocumentDbSchema(DATABASE_NAME, 1, metadata);
        final DocumentDbSchemaTable schemaTable = metadata.get(COLLECTION_NAME);
        Assertions.assertNotNull(schemaTable);
        tableId = schemaTable.getId();

        final FileSchemaWriter writer = new FileSchemaWriter(DATABASE_NAME);
        writer.write(schema, metadata.values());
    }

    @AfterAll
    static void afterAll() {
        final SchemaWriter schemaWriter = new FileSchemaWriter(DATABASE_NAME);
        schemaWriter.remove(DEFAULT_SCHEMA_NAME);
    }

    @DisplayName("Test can read default schema")
    @Test
    void testReadDefaultSchema() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        final DocumentDbSchema schema = schemaReader.read();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Tests can read the latest version a specific schema.")
    @Test
    void testReadSchema() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        final DocumentDbSchema schema = schemaReader.read(DEFAULT_SCHEMA_NAME);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Tests can read a specific schema and version.")
    @Test
    void testReadSchemaWithVersion() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        final DocumentDbSchema schema = schemaReader.read(DEFAULT_SCHEMA_NAME, 1);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Tests can read a specific table schema.")
    @Test
    void testReadSchemaTable() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        final DocumentDbSchemaTable schemaTable = schemaReader.readTable(DEFAULT_SCHEMA_NAME, 1, tableId);
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(tableId, schemaTable.getId());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getSqlName());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getCollectionName());
        Assertions.assertNotNull(schemaTable.getColumnMap());
        Assertions.assertEquals(13, schemaTable.getColumnMap().size());
    }

    // Error handling ...
    @DisplayName("Tests cannot read unknown schema name.")
    @Test
    void testReadSchemaNotExist() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        Assertions.assertNull(schemaReader.read("unknown"));
    }

    @DisplayName("Tests cannot read unknown schema version.")
    @Test
    void testReadSchemaWithVersionNotExist() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        Assertions.assertNull(schemaReader.read(DEFAULT_SCHEMA_NAME, 100));
    }

    @DisplayName("Tests cannot read unknown table schema.")
    @Test
    void testReadSchemaTableNotExist() {
        final SchemaReader schemaReader = new FileSchemaReader(DATABASE_NAME);
        Assertions.assertEquals("Given table ID 'unknown' is not found.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> schemaReader.readTable(
                                DEFAULT_SCHEMA_NAME, 1, "unknown"))
                        .getMessage());
    }
}
