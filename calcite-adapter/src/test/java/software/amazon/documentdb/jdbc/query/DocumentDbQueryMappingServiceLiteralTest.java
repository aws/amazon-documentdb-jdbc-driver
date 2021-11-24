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
    void testWhereQueryForObjectId() throws SQLException {
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

    @Test
    @DisplayName("Tests querying for ObjectId type in SELECT clause.")
    void testSelectQueryForObjectId() throws SQLException {
        final String query1 =
                String.format("SELECT %2$s__id = '%3$s' FROM %1$s.%2$s",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME,
                        BSON_OBJECT_ID.getValue().toHexString());
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": [{"
                                + "\"$and\": ["
                                + "{\"$gt\": [\"$_id\", null]}, "
                                + "{\"$gt\": [{\"$literal\": \"123456789012345678901234\"}, null]}]}, "
                                + "{\"$or\": ["
                                + "{\"$eq\": [\"$_id\", {\"$oid\": \"123456789012345678901234\"}]}, "
                                + "{\"$eq\": [\"$_id\", {\"$literal\": \"123456789012345678901234\"}]}]}, null]}, "
                                + "\"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        // Hex string
        final String query2 =
                String.format("SELECT %2$s__id =x'%3$s' FROM %1$s.%2$s",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME,
                        BSON_OBJECT_ID.getValue().toHexString());
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": ["
                                + "{\"$and\": ["
                                + "{\"$gt\": [\"$_id\", null]}, "
                                + "{\"$gt\": [{\"$binary\": {\"base64\": \"EjRWeJASNFZ4kBI0\", \"subType\": \"00\"}}, null]}]}, "
                                + "{\"$or\": ["
                                + "{\"$eq\": [\"$_id\", {\"$oid\": \"123456789012345678901234\"}]}, "
                                + "{\"$eq\": [\"$_id\", {\"$binary\": {\"base64\": \"EjRWeJASNFZ4kBI0\", \"subType\": \"00\"}}]}]}, null]}, "
                                + "\"_id\": 0}}"),
                result2.getAggregateOperations().get(0));

        // String
        final String query3 =
                String.format("SELECT %2$s__id = 'arbitrary string' FROM %1$s.%2$s",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(1, result3.getColumnMetaData().size());
        Assertions.assertEquals(1, result3.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [\"$_id\", null]}, "
                                + "{\"$gt\": [{\"$literal\": \"arbitrary string\"}, null]}]}, "
                                + "{\"$eq\": [\"$_id\", {\"$literal\": \"arbitrary string\"}]}, null]}, "
                                + "\"_id\": 0}}"),
                result3.getAggregateOperations().get(0));

        // Long integer
        final String query4 =
                String.format("SELECT %2$s__id = 4223372036854775807 FROM %1$s.%2$s",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result4 = queryMapper.get(query4);
        Assertions.assertNotNull(result4);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result4.getCollectionName());
        Assertions.assertEquals(1, result4.getColumnMetaData().size());
        Assertions.assertEquals(1, result4.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": ["
                                + "{\"$and\": ["
                                + "{\"$gt\": [\"$_id\", null]}, "
                                + "{\"$gt\": [{\"$literal\": {\"$numberLong\": \"4223372036854775807\"}}, null]}]}, "
                                + "{\"$eq\": [\"$_id\", {\"$literal\": {\"$numberLong\": \"4223372036854775807\"}}]}, null]}, \"_id\": 0}}"),
                result4.getAggregateOperations().get(0));

        // Byte array
        final String query5 =
                String.format("SELECT %2$s__id = x'0123456789abcdef' FROM %1$s.%2$s",
                        getDatabaseName(),
                        OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result5 = queryMapper.get(query5);
        Assertions.assertNotNull(result5);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result5.getCollectionName());
        Assertions.assertEquals(1, result5.getColumnMetaData().size());
        Assertions.assertEquals(1, result5.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": ["
                                + "{\"$and\": ["
                                + "{\"$gt\": [\"$_id\", null]}, "
                                + "{\"$gt\": [{\"$binary\": {\"base64\": \"ASNFZ4mrze8=\", \"subType\": \"00\"}}, null]}]}, "
                                + "{\"$eq\": [\"$_id\", {\"$binary\": {\"base64\": \"ASNFZ4mrze8=\", \"subType\": \"00\"}}]}, null]}, "
                                + "\"_id\": 0}}"),
                result5.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Tests that all supported literal types are returned correctly.")
    void testLiteralTypes() throws SQLException {
        // Boolean literals
        final String query1 =
                String.format(
                        "SELECT TRUE AS \"literalTrue\", "
                                + "FALSE AS \"literalFalse\", "
                                + "UNKNOWN AS \"literalUnknown\" "
                                + "FROM \"%s\".\"%s\"",
                        getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(3, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalTrue\": {\"$literal\": true}, "
                                + "\"literalFalse\": {\"$literal\": false}, "
                                + "\"literalUnknown\": null, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        // Numeric literals
        final String query2 = String.format(
                "SELECT CAST(-128 AS TINYINT) AS \"literalTinyInt\", "
                        + "CAST(-32768 AS SMALLINT) AS \"literalSmallInt\", "
                        + "CAST(-2147483648 AS INT) AS \"literalInt\", "
                        + "CAST(-9223372036854775808 AS BIGINT) AS \"literalBigInt\", "
                        + "CAST(123.45 AS DECIMAL(5, 2)) AS \"literalDecimal\", "
                        + "CAST(123.45 AS NUMERIC(5, 2)) AS \"literalNumeric\", "
                        + "CAST(1234.56 AS FLOAT) AS \"literalFloat\", "
                        + "CAST(12345.678 AS REAL) AS \"literalReal\", "
                        + "CAST(12345.6789999999999 AS DOUBLE) AS \"literalDouble\""
                        + "FROM \"%s\".\"%s\"",
                getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(9, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalTinyInt\": {\"$literal\": {\"$numberInt\": \"-128\"}}, "
                                + "\"literalSmallInt\": {\"$literal\": {\"$numberInt\": \"-32768\"}}, "
                                + "\"literalInt\": {\"$literal\": {\"$numberInt\": \"-2147483648\"}}, "
                                + "\"literalBigInt\": {\"$literal\": {\"$numberLong\": \"-9223372036854775808\"}}, "
                                + "\"literalDecimal\": {\"$literal\": {\"$numberDouble\": \"123.45\"}}, "
                                + "\"literalNumeric\": {\"$literal\": {\"$numberDouble\": \"123.45\"}}, "
                                + "\"literalFloat\": {\"$literal\": {\"$numberDouble\": \"1234.56\"}}, "
                                + "\"literalReal\": {\"$literal\": {\"$numberDouble\": \"12345.678\"}}, "
                                + "\"literalDouble\": {\"$literal\": {\"$numberDouble\": \"12345.679\"}}, "
                                + "\"_id\": 0}}"),
                 result2.getAggregateOperations().get(0));

        // String literals
        final String query3 =
                String.format(
                        "SELECT CAST('Hello' AS CHAR(5)) AS \"literalChar\", "
                                + "CAST('' AS CHAR(5)) AS \"literalCharEmpty\", "
                                + "CAST('Hello' AS VARCHAR) AS \"literalVarchar\", "
                                + "CAST('' AS VARCHAR) AS \"literalVarcharEmpty\" "
                                + "FROM \"%s\".\"%s\"",
                        getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(4, result3.getColumnMetaData().size());
        Assertions.assertEquals(1, result3.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalChar\": {\"$literal\": \"Hello\"}, "
                                + "\"literalCharEmpty\": {\"$literal\": \"     \"}, "
                                + "\"literalVarchar\": {\"$literal\": \"Hello\"}, "
                                + "\"literalVarcharEmpty\": {\"$literal\": \"\"}, "
                                + "\"_id\": 0}}"),
                result3.getAggregateOperations().get(0));

        // Binary literals
        final String query4 =
                String.format(
                        "SELECT CAST(x'45F0AB' AS BINARY(3)) AS \"literalBinary\", "
                                + "CAST(x'' AS BINARY(3)) AS \"literalBinaryEmpty\", "
                                + "CAST(x'45F0AB' AS VARBINARY) AS \"literalVarbinary\", "
                                + "CAST(x'' AS VARBINARY) AS \"literalVarbinaryEmpty\" "
                                + "FROM \"%s\".\"%s\"",
                        getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result4 = queryMapper.get(query4);
        Assertions.assertNotNull(result4);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result4.getCollectionName());
        Assertions.assertEquals(4, result4.getColumnMetaData().size());
        Assertions.assertEquals(1, result4.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalBinary\": {\"$binary\": {\"base64\": \"RfCr\", \"subType\": \"00\"}}, "
                                + "\"literalBinaryEmpty\": {\"$binary\": {\"base64\": \"AAAA\", \"subType\": \"00\"}}, "
                                + "\"literalVarbinary\": {\"$binary\": {\"base64\": \"RfCr\", \"subType\": \"00\"}}, "
                                + "\"literalVarbinaryEmpty\": {\"$binary\": {\"base64\": \"\", \"subType\": \"00\"}}, "
                                + "\"_id\": 0}}"),
                result4.getAggregateOperations().get(0));

        // Date/time literals
        final String query5 =
                String.format(
                        "SELECT TIME '20:17:40' AS \"literalTime\", "
                                + "DATE '2017-09-20' AS \"literalDate\", "
                                + "TIMESTAMP '2017-09-20 20:17:40' AS \"literalTimestamp\""
                                + "FROM \"%s\".\"%s\"",
                        getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result5 = queryMapper.get(query5);
        Assertions.assertNotNull(result5);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result5.getCollectionName());
        Assertions.assertEquals(3, result5.getColumnMetaData().size());
        Assertions.assertEquals(1, result5.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalTime\": {\"$date\": {\"$numberLong\": \"73060000\" }}, "
                                + "\"literalDate\": {\"$date\": {\"$numberLong\": \"1505865600000\" }}, "
                                + "\"literalTimestamp\": {\"$date\": {\"$numberLong\": \"1505938660000\" }}, "
                                + "\"_id\": 0}}"),
                result5.getAggregateOperations().get(0));

        // Interval literals
        final String query6 =
                String.format(
                        "SELECT INTERVAL '123-2' YEAR(3) TO MONTH AS \"literalYearToMonth\", "
                                + "INTERVAL '123' YEAR(3) AS \"literalYear\", "
                                + "INTERVAL 300 MONTH(3) AS \"literalMonth\", "
                                + "INTERVAL '400' DAY(3) AS \"literalDay\", "
                                + "INTERVAL '400 5' DAY(3) TO HOUR AS \"literalDayToHour\", "
                                + "INTERVAL '4 5:12' DAY TO MINUTE AS \"literalDayToMinute\", "
                                + "INTERVAL '4 5:12:10.789' DAY TO SECOND AS \"literalDayToSecond\", "
                                + "INTERVAL '10' HOUR AS \"literalHour\", "
                                + "INTERVAL '11:20' HOUR TO MINUTE AS \"literalHourToMinute\", "
                                + "INTERVAL '11:20:10' HOUR TO SECOND AS \"literalHourToSecond\", "
                                + "INTERVAL '10' MINUTE AS \"literalMinute\", "
                                + "INTERVAL '10:22' MINUTE TO SECOND AS \"literalMinuteToSecond\", "
                                + "INTERVAL '30' SECOND AS \"literalSecond\""
                                + "FROM \"%s\".\"%s\"",
                        getDatabaseName(), OBJECT_ID_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result6 = queryMapper.get(query6);
        Assertions.assertNotNull(result6);
        Assertions.assertEquals(OBJECT_ID_COLLECTION_NAME, result6.getCollectionName());
        Assertions.assertEquals(13, result6.getColumnMetaData().size());
        Assertions.assertEquals(1, result6.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"literalYearToMonth\": {\"$literal\": 1478}, "
                                + "\"literalYear\": {\"$literal\": 1476}, "
                                + "\"literalMonth\": {'$multiply': [{\"$literal\": {\"$numberInt\": \"300\"}}, {\"$literal\": 1}]}, "
                                + "\"literalDay\": {\"$literal\": {\"$numberLong\": \"34560000000\"}}, "
                                + "\"literalDayToHour\": {\"$literal\": {\"$numberLong\": \"34578000000\"}}, "
                                + "\"literalDayToMinute\": {\"$literal\": {\"$numberLong\": \"364320000\"}}, "
                                + "\"literalDayToSecond\": {\"$literal\": {\"$numberLong\": \"364330789\"}}, "
                                + "\"literalHour\": {\"$literal\": {\"$numberLong\": \"36000000\"}}, "
                                + "\"literalHourToMinute\": {\"$literal\": {\"$numberLong\": \"40800000\"}}, "
                                + "\"literalHourToSecond\": {\"$literal\": {\"$numberLong\": \"40810000\"}}, "
                                + "\"literalMinute\": {\"$literal\": {\"$numberLong\": \"600000\"}}, "
                                + "\"literalMinuteToSecond\": {\"$literal\": {\"$numberLong\": \"622000\"}}, "
                                + "\"literalSecond\": {\"$literal\": {\"$numberLong\": \"30000\"}}, "
                                + "_id: 0}}"),
                result6.getAggregateOperations().get(0));
    }
}
