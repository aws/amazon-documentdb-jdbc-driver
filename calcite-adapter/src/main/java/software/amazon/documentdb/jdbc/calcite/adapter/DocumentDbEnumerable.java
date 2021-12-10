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

package software.amazon.documentdb.jdbc.calcite.adapter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * Initially, aggregate and find returned anonymous classes as the enumerable in CalciteSignature.
 * Returning this instead, allows us to get more information from CalciteSignature.
 */
@Getter
@AllArgsConstructor
public class DocumentDbEnumerable extends AbstractEnumerable<Object> {

    private final String databaseName;
    private final String collectionName;
    private final List<Bson> list;
    private final List<String> paths;

    @Override
    public Enumerator<Object> enumerator() {
        // Implement the enumerable interface but do not execute query.
        return new DocumentDbEnumerator(null);
    }
}
