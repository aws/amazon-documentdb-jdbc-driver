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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class DocumentDbSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(final SchemaPlus parentSchema, final String name, final Map<String, Object> operand) {
        final DocumentDbConnectionProperties properties = getProperties(operand);
        final MongoDatabase database = getDatabase(properties);

        return new DocumentDbSchema(database);
    }

    private static MongoDatabase getDatabase(final DocumentDbConnectionProperties properties) {
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        final MongoClient client = MongoClients.create(settings);

        return client.getDatabase(properties.getDatabase());
    }

    private static DocumentDbConnectionProperties getProperties(final Map<String, Object> operand) {
        final Properties info = new Properties();
        for (Entry<String, Object> entry : operand.entrySet()) {
            info.put(entry.getKey(), entry.getValue());
        }

        return new DocumentDbConnectionProperties(info);
    }
}
