/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.metadata;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonMapSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.model.JsonView;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbSchemaFactory;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Provides a way to scan metadata in DocumentDB collections
 */
public class DocumentDbMetadataScanner {
    private static final String DEFAULT_SCHEMA_NAME = "mongo";
    private static final String DEFAULT_DATABASE_SCHEMA_NAME_PREFIX = DEFAULT_SCHEMA_NAME + "_";
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMetadataScanner.class);

    /**
     * Create a {@link JsonRoot} for a view model of the collections in the target database.
     * @param properties the connection properties.
     * @return a {@link JsonRoot} with the view model set.
     */
    public static JsonRoot createViewModel(final DocumentDbConnectionProperties properties)
            throws SQLException {
        // TODO: Generalize
        final int limit = 1;

        final JsonRoot rootModel = new JsonRoot();
        rootModel.version = "1.0";
        rootModel.defaultSchema = DEFAULT_SCHEMA_NAME;
        final JsonCustomSchema customSchema = new JsonCustomSchema();
        customSchema.name = DEFAULT_DATABASE_SCHEMA_NAME_PREFIX + properties.getDatabase();
        customSchema.factory = DocumentDbSchemaFactory.class.getName();
        customSchema.operand = new HashMap<>();

        // Copy properties into the "operand" which is passed to the custom Schema factory.
        for (Entry<Object, Object> entry : properties.entrySet()) {
            customSchema.operand.put(
                    String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        rootModel.schemas.add(customSchema);
        final JsonMapSchema mapSchema = new JsonMapSchema();
        mapSchema.name = rootModel.defaultSchema;
        final MongoClientSettings settings =  properties.buildMongoClientSettings(null);

        try (MongoClient client = MongoClients.create(settings)) {
            final MongoDatabase database = client.getDatabase(properties.getDatabase());
            pingDatabase(database);

            final MongoIterable<String> collectionNames = database.listCollectionNames();

            // Create maps for each collection
            for (String collectionName : collectionNames) {
                final MongoCollection<BsonDocument> collection = database.getCollection(
                        collectionName, BsonDocument.class);
                final FindIterable<BsonDocument> documents = collection.find().limit(limit);
                final MongoCursor<BsonDocument> cursor = documents.cursor();
                JsonView table = null;

                // TODO: Generalize for various discovery themes.
                // Iterate through the document(s)
                while (cursor.hasNext()) {
                    final BsonDocument document = cursor.next();
                    table = createJsonView(document, collectionName, customSchema.name);
                    break;
                }
                mapSchema.tables.add(table);
            }
        }

        rootModel.schemas.add(mapSchema);
        return rootModel;
    }

    private static JsonView createJsonView(
            final BsonDocument document,
            final String collectionName,
            final String schemaName) {

        // TODO: Generalize for various discovery themes.
        final JsonView tableView = new JsonView();
        tableView.name = collectionName;
        final String viewTemplate = "select %s from \"%s\".\"%s\"";
        final StringBuilder columnBuilder = new StringBuilder();

        // Iterate through the fields
        for (Entry<String, BsonValue> entry : document.entrySet()) {
            appendColumnMap(columnBuilder, entry.getKey(), entry.getValue().getBsonType());
        }

        tableView.sql = String.format(viewTemplate,
                columnBuilder.toString(), schemaName, collectionName);

        return tableView;
    }

    private static void appendColumnMap(final StringBuilder columnBuilder, final String columnName,
            final BsonType bsonType) {
        final boolean addComma = columnBuilder.length() != 0;
        final String columnTemplate = "cast(_MAP['%s'] AS %s) AS \"%s\"";
        final String columnWithPrecisionTemplate = "cast(_MAP['%s'] AS %s(%s)) AS \"%s\"";

        final SqlTypeName sqlTypeName = toSqlTypeName(bsonType);
        if (addComma) {
            columnBuilder.append(", ");
        }

        switch (sqlTypeName) {
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INTEGER:
            case TIMESTAMP:
                columnBuilder.append(String.format(columnTemplate,
                        columnName, sqlTypeName.getName(), columnName));
                break;
            case VARBINARY:
                columnBuilder.append(String.format(columnWithPrecisionTemplate,
                        columnName, "VARBINARY", Integer.MAX_VALUE, columnName));
                break;
            case VARCHAR:
                switch (bsonType) {
                    case MAX_KEY:
                    case MIN_KEY:
                        columnBuilder.append(String.format(columnWithPrecisionTemplate,
                                columnName, sqlTypeName.getName(), 10, columnName));
                        break;
                    case STRING:
                    case NULL:
                        columnBuilder.append(String.format(columnWithPrecisionTemplate,
                                columnName, sqlTypeName.getName(), 0x1000000, columnName));
                        break;
                    case OBJECT_ID:
                        columnBuilder.append(String.format(columnWithPrecisionTemplate,
                                columnName, sqlTypeName.getName(), 32, columnName));
                        break;
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static SqlTypeName toSqlTypeName(final BsonType bsonType) {
        switch (bsonType) {
            case BINARY:
                return SqlTypeName.VARBINARY;
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case DATE_TIME:
                return SqlTypeName.TIMESTAMP;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case INT32:
                return  SqlTypeName.INTEGER;
            case INT64:
                return SqlTypeName.BIGINT;
            case MAX_KEY:
            case MIN_KEY:
            case OBJECT_ID:
            case NULL:
            case STRING:
                return SqlTypeName.VARCHAR;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Attempts to ping the database.
     *
     * @throws SQLException if connecting to the database fails for any reason.
     */
    private static void pingDatabase(final MongoDatabase mongoDatabase) throws SQLException {
        try {
            mongoDatabase.runCommand(new Document("ping", 1));
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }
}
