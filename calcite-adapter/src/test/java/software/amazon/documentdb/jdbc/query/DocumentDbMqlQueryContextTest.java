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

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DocumentDbMqlQueryContextTest {

    @Test
    @DisplayName("Tests that aggregate operations are correctly converted from BSON document to string in getter.")
    void testGetAggregateOperationsAsStrings() {
        final List<String> stages = new ArrayList<>();
        // Generic stage
        stages.add(
                "{\"$unwind\": {"
                        + "\"path\": \"$array\", "
                        + "\"includeArrayIndex\": \"array_index_lvl_0\", "
                        + "\"preserveNullAndEmptyArrays\": true}}");
        // Stage with 3-valued logic (many null checks)
        stages.add(
                "{\"$project\": {"
                        + "\"booleanField\": "
                        + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                        + "{\"$gt\": [\"$array.field2\", null]}]}, "
                        + "{\"$eq\": [\"$array.field\", \"$array.field2\"]}, null]}}}");
        // Stage with different Bson types
        stages.add(
                "{\"$project\": {"
                        + "\"literalNull\": {\"$literal\": null}, "
                        + "\"literalTimestamp\": {\"$date\": {\"$numberLong\": \"1505938660000\"}}, "
                        + "\"literalInt\": {\"$literal\": {\"$numberInt\": \"-2147483648\"}}, "
                        + "\"literalDecimal\": {\"$literal\": {\"$numberDouble\": \"123.45\"}}, "
                        + "\"literalVarchar\": {\"$literal\": \"Hello! 你好!\"}, "
                        + "\"literalBinary\": {\"$binary\": {\"base64\": \"RfCr\", \"subType\": \"00\"}}}}");
        // Stage with a lot of nesting and aggregate operators
        stages.add(
                "{\"$project\": {\"EXPR$0\": {\"$substrCP\": [\"$array.field\", "
                        + "{\"$subtract\": [\"$array.field2\", {\"$numberInt\": \"1\"}]}, "
                        + "{\"$subtract\": [\"$array.field1\", \"$array.field2\"]}]}, "
                        + "\"_id\": {\"$numberInt\": \"0\"}}}");

        final DocumentDbMqlQueryContext context =
                DocumentDbMqlQueryContext.builder()
                        .aggregateOperations(
                                stages.stream().map(BsonDocument::parse).collect(Collectors.toList()))
                        .build();
        Assertions.assertEquals(stages.size(), context.getAggregateOperationsAsStrings().size());
        for (int i = 0; i < stages.size(); i++) {
            Assertions.assertEquals(stages.get(i), context.getAggregateOperationsAsStrings().get(i));
        }
    }
}
