/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * @return a new {@link Schema} for the database.
     */
    public Schema create(final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final DocumentDbConnectionProperties properties) {
        return new DocumentDbSchema(databaseMetadata, properties);
    }
}
