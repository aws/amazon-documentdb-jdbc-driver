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

import com.mongodb.client.MongoDatabase;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import software.amazon.documentdb.jdbc.common.utilities.LazyLinkedHashMap;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Provides a schema for DocumentDB
 */
public class DocumentDbSchema extends AbstractSchema {
    private final MongoDatabase mongoDatabase;
    private Map<String, Table> tables;
    private final DocumentDbDatabaseSchemaMetadata databaseMetadata;

    /**
     * Constructs a new {@link DocumentDbSchema} from {@link DocumentDbDatabaseSchemaMetadata}.
     *
     * @param databaseMetadata the database metadata.
     */
    protected DocumentDbSchema(final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final MongoDatabase mongoDatabase) {
        this.databaseMetadata = databaseMetadata;
        this.mongoDatabase = mongoDatabase;
        tables = null;
    }

    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    @SneakyThrows
    @Override
    protected Map<String, Table> getTableMap() {
        if (tables == null) {
            tables = new LazyLinkedHashMap<>(
                    new LinkedHashSet<>(databaseMetadata.getTableSchemaMap().keySet()),
                    tableName -> {
                        final DocumentDbSchemaTable schemaTable = databaseMetadata
                                .getTableSchemaMap().get(tableName);
                        return new DocumentDbTable(schemaTable.getCollectionName(), schemaTable);
                    });
        }
        return tables;
    }
}
