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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.DEFAULT_SCHEMA_NAME;

class DocumentDbSchemaTest {
    private static final String COLLECTION_NAME = DocumentDbTableSchemaGeneratorTest.class.getSimpleName();
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .serializationInclusion(Include.NON_NULL)
            .serializationInclusion(Include.NON_EMPTY)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .defaultDateFormat(new StdDateFormat().withColonInTimeZone(true))
            // Enable fail on unknown properties to ensure exact interface match
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT)
            // Make the enums lower case.
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .addModule(buildEnumLowerCaseSerializerModule())
            .addModule(new GuavaModule())
            .build();

    private static SimpleModule buildEnumLowerCaseSerializerModule() {
        final SimpleModule module = new SimpleModule();
        final JsonSerializer<Enum> serializer = new StdSerializer<Enum>(Enum.class) {
            @Override
            public void serialize(final Enum value, final JsonGenerator jGen,
                    final SerializerProvider provider) throws IOException {
                jGen.writeString(value.name().toLowerCase());
            }
        };
        module.addSerializer(Enum.class, serializer);
        return module;
    }

    @DisplayName("Tests deserialization of schema.")
    @Test
    void testDeserialize() throws JsonProcessingException, DocumentDbSchemaException {
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
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documentList.iterator());
        final DocumentDbSchema schema1 = new DocumentDbSchema(
                "testDb", 1, metadata);

        // Serialize/deserialize the object.
        final String schemaJson = OBJECT_MAPPER.writeValueAsString(schema1);
        final DocumentDbSchema schema2 = OBJECT_MAPPER.readValue(
                schemaJson,
                DocumentDbSchema.class);
        Assertions.assertNotNull(schema2);
        // Use the original collection to lazy load the tables.
        schema2.setGetTableFunction(
                tableId -> schema1.getTableMap().get(tableId.split("[:][:]")[0]),
                remaining -> remaining.stream()
                        .collect(Collectors.toMap(
                                tableId -> tableId,
                                tableId -> schema1.getTableMap().get(tableId),
                                (a, b) -> b,
                                LinkedHashMap::new)));
        Assertions.assertEquals(1, schema2.getTableMap().size());
        Assertions.assertEquals(schema1.getTableMap().get(COLLECTION_NAME), schema2.getTableMap().get(COLLECTION_NAME));
        Assertions.assertEquals(schema1, schema2); // Performs a member-wise check

        for (DocumentDbSchemaTable tableSchema : schema1.getTableMap().values()) {
            final String tableSchemaJson = OBJECT_MAPPER.writeValueAsString(tableSchema);
            final DocumentDbSchemaTable deserializedTableSchema = OBJECT_MAPPER.readValue(
                    tableSchemaJson, DocumentDbSchemaTable.class);
            // Note this is reversed because deserializedTableSchema is of type DocumentDbSchemaTable
            // but tableSchema is of type DocumentDbMetadataTable.
            Assertions.assertEquals(deserializedTableSchema, tableSchema);
        }
    }

    @DisplayName("Tests serialization of schema.")
    @Test
    void testSerialize() throws JsonProcessingException {
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
        final Map<String, DocumentDbSchemaTable> metadata = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, documentList.iterator());
        final DocumentDbSchema schema1 = new DocumentDbSchema(
                "testDb", 1, metadata);

        @SuppressWarnings("unchecked")
        final Map<String, Object> schemaMap = OBJECT_MAPPER.convertValue(schema1, Map.class);
        final List<String> keys = Arrays.asList("schemaName", "sqlName", "schemaVersion", "modifyDate", "tables");
        Assertions.assertTrue(schemaMap.keySet().containsAll(keys));
        Assertions.assertTrue(keys.containsAll(schemaMap.keySet()));
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schemaMap.get("schemaName"));
        Assertions.assertEquals("testDb", schemaMap.get("sqlName"));
        Assertions.assertEquals(1, schemaMap.get("schemaVersion"));
        Assertions.assertTrue(schemaMap.get("modifyDate") instanceof String);
        Assertions.assertTrue(schemaMap.get("tables") instanceof List<?>);
        @SuppressWarnings("unchecked")
        final List<String> tables = (List<String>) schemaMap.get("tables");
        Assertions.assertEquals(1, tables.size());
        final String tableId = tables.get(0);
        Assertions.assertTrue(tableId.startsWith(COLLECTION_NAME + "::"));

        for (DocumentDbSchemaTable table : schema1.getTableMap().values()) {
            final List<String> tableKeys = Arrays.asList("uuid", "sqlName", "collectionName",
                    "modifyDate", "columns", "_id");
            @SuppressWarnings("unchecked")
            final Map<String, Object> tableMap = OBJECT_MAPPER.convertValue(
                    table, Map.class);
            Assertions.assertTrue(tableMap.keySet().containsAll(tableKeys));
            Assertions.assertTrue(tableKeys.containsAll(tableMap.keySet()));
            Assertions.assertEquals(tableId, tableMap.get("_id"));
            Assertions.assertEquals(tableId.split("[:][:]")[1], tableMap.get("uuid"));
            Assertions.assertEquals(COLLECTION_NAME, tableMap.get("sqlName"));
            Assertions.assertEquals(COLLECTION_NAME, tableMap.get("collectionName"));
            Assertions.assertTrue(tableMap.get("columns") instanceof List<?>);
            @SuppressWarnings("unchecked")
            final List<DocumentDbSchemaColumn> columns = (List<DocumentDbSchemaColumn>) tableMap.get("columns");
            Assertions.assertEquals(13, columns.size());
        }
    }

    @DisplayName("Tests equals() method with different combinations.")
    @Test
    void testEquals() {
        final Date date = new Date(100);
        final Date otherDate = new Date(200);
        final Set<String> tables = new LinkedHashSet<>();
        tables.add("table");
        final DocumentDbSchema schema1 = new DocumentDbSchema("_default", 1, "testDb", date, null);
        final DocumentDbSchema schema2 = new DocumentDbSchema("_default", 1, "testDb", date, null);
        final DocumentDbSchema schema3 = new DocumentDbSchema("_default", 2, "testDb", date, null);
        final DocumentDbSchema schema4 = new DocumentDbSchema("_other", 1, "testDb", date, null);
        final DocumentDbSchema schema5 = new DocumentDbSchema("_default", 1, "otherTestDb", date, null);
        final DocumentDbSchema schema6 = new DocumentDbSchema("_default", 1, "testDb", otherDate, null);
        final DocumentDbSchema schema7 = new DocumentDbSchema("_default", 1, "testDb", date, tables);

        Assertions.assertTrue(schema1.equals(schema1));
        Assertions.assertTrue(schema1.equals(schema2));
        Assertions.assertFalse(schema1.equals(schema3));
        Assertions.assertFalse(schema1.equals(schema4));
        Assertions.assertFalse(schema1.equals(schema5));
        Assertions.assertFalse(schema1.equals(schema6));
        Assertions.assertFalse(schema1.equals(schema7));
        Assertions.assertFalse(schema1.equals(new Object()));
    }
}
