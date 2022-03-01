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

import lombok.Builder;
import lombok.Getter;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is meant to carry all the information needed to execute the query in DocumentDB and
 * construct a ResultSet.
 */
@Getter
@Builder
public class DocumentDbMqlQueryContext {
    /** The column metadata describing the return row. */
    private final List<JdbcColumnMetaData> columnMetaData;
    /** The operations to use in the aggregation. */
    private final List<Bson> aggregateOperations;
    /** The collection name to use in the aggregation. */
    private final String collectionName;
    /** The path information for the output documents. Maps column names to field paths.*/
    private final List<String> paths;

    /**
     * Gets the aggregation operations (stages) for the query as a list of strings.
     *
     * @return the aggregation operations as an ordered list of strings in extended JSON format.
     */
    public List<String> getAggregateOperationsAsStrings() {
        return aggregateOperations.stream()
                .map(
                        doc ->
                                doc.toBsonDocument()
                                        .toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()))
                .collect(Collectors.toList());
    }
}
