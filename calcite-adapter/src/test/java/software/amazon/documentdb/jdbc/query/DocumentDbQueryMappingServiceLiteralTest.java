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

package software.amazon.documentdb.jdbc.query;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbFilter;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;

import java.sql.SQLException;
import java.time.Instant;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceLiteralTest extends DocumentDbQueryMappingServiceTest {
    private static final String OBJECT_ID_COLLECTION_NAME = "objectIdCollection";
    private static final BsonObjectId BSON_OBJECT_ID = new BsonObjectId(
            new ObjectId("123456789012345678901234"));
    private static DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void initialize() throws SQLException {
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        final BsonDocument objectIdDocument = new BsonDocument("_id", BSON_OBJECT_ID)
                .append("field", new BsonString("value"))
                .append("dateField", new BsonDateTime(dateTime));
        insertBsonDocuments(OBJECT_ID_COLLECTION_NAME, new BsonDocument[]{objectIdDocument});
        queryMapper = getQueryMappingService();
    }

    @Test
    @DisplayName("Tests that querying for ObjectId type.")
    void testQueryForObjectId() throws SQLException {
        final String query1 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id = '%3$s'",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME,
                        BSON_OBJECT_ID.getValue().toHexString());
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(2, result1.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"objectIdCollection__id\": {\"$eq\": {\"$oid\": \"123456789012345678901234\"}}}, "
                                + "{\"objectIdCollection__id\": {\"$eq\": \"123456789012345678901234\"}}]}}"),
                result1.getAggregateOperations().get(1));

        // In-memory substring and concatenation.
        final String query2 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id ="
                                + " CONCAT(SUBSTRING('%3$s', 1, 10), SUBSTRING('%3$s', 11))",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME,
                        BSON_OBJECT_ID.getValue().toHexString());
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(2, result2.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"objectIdCollection__id\": {\"$eq\": {\"$oid\": \"123456789012345678901234\"}}}, "
                                + "{\"objectIdCollection__id\": {\"$eq\": \"123456789012345678901234\"}}]}}"),
                result1.getAggregateOperations().get(1));

        // Hex string
        final String query3 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id ="
                                + " x'%3$s'",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME,
                        BSON_OBJECT_ID.getValue().toHexString());
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(1, result3.getColumnMetaData().size());
        Assertions.assertEquals(2, result3.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"objectIdCollection__id\": {\"$eq\": {\"$oid\": \"123456789012345678901234\"}}}, "
                                + "{\"objectIdCollection__id\": {\"$eq\": {\"$binary\": {\"base64\": \"EjRWeJASNFZ4kBI0\", \"subType\": \"00\"}}}}]}}"),
                result3.getAggregateOperations().get(1));

        // String
        final String query4 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id ="
                                + " 'arbitrary string'",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result4 = queryMapper.get(query4);
        Assertions.assertNotNull(result4);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result4.getCollectionName());
        Assertions.assertEquals(1, result4.getColumnMetaData().size());
        Assertions.assertEquals(2, result4.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result4.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"objectIdCollection__id\": {\"$eq\": \"arbitrary string\"}}}"),
                result4.getAggregateOperations().get(1));

        // Long integer
        final String query5 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id ="
                                + " 4223372036854775807",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result5 = queryMapper.get(query5);
        Assertions.assertNotNull(result5);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result5.getCollectionName());
        Assertions.assertEquals(1, result5.getColumnMetaData().size());
        Assertions.assertEquals(2, result5.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result5.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"objectIdCollection__id\": {\"$eq\": 4223372036854775807}}}"),
                result5.getAggregateOperations().get(1));
        // Byte array
        final String query6 =
                String.format("SELECT %2$s__id FROM %1$s.%2$s WHERE %2$s__id ="
                                + " x'0123456789abcdef'",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result6 = queryMapper.get(query6);
        Assertions.assertNotNull(result6);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result6.getCollectionName());
        Assertions.assertEquals(1, result6.getColumnMetaData().size());
        Assertions.assertEquals(2, result6.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"objectIdCollection__id\": \"$_id\", \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"objectIdCollection__id\": {\"$eq\": {\"$binary\": {\"base64\": \"ASNFZ4mrze8=\", \"subType\": \"00\"}}}}}"),
                result6.getAggregateOperations().get(1));
    }
}
