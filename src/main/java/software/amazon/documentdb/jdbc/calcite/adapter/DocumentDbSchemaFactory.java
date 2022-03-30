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

import org.apache.calcite.schema.Schema;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

public class DocumentDbSchemaFactory {
    /**
     * Creates {@link Schema} from database metadata.
     *
     * @param databaseMetadata the database metadata.
     * @param connectionProperties the connection properties.
     * @return a new {@link Schema} for the database.
     */
    public static Schema create(final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final DocumentDbConnectionProperties connectionProperties) {
        return new DocumentDbSchema(databaseMetadata, connectionProperties);
    }
}
