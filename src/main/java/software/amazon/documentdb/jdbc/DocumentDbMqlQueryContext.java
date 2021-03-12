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

package software.amazon.documentdb.jdbc;

import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.bson.conversions.Bson;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;
import java.util.List;

/**
 * This is meant to carry
 * all the information needed to execute the query in MongoDb and
 * construct a ResultSet.
 */
@Getter
@Builder
public class DocumentDbMqlQueryContext {
    /** The column metadata (a Calcite interface) describing the return row. */
    private final List<ColumnMetaData> columnMetaData;
    /** The operations to use in the aggregation. */
    private final List<Bson> aggregateOperations;
    /** The table metadata (perhaps this needs to be a list?) */
    private final DocumentDbMetadataTable metadataTable;
    /** The collection name to use in the aggregation. */
    private final String collectionName;
}
