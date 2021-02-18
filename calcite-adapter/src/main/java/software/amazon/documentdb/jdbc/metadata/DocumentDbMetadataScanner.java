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

package software.amazon.documentdb.jdbc.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonMapSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.model.JsonView;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbMetadataScanMethod;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbSchemaFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Provides a way to scan metadata in DocumentDB collections
 */
public class DocumentDbMetadataScanner {
    private static final String DEFAULT_SCHEMA_NAME = "mongo";
    private static final String DEFAULT_DATABASE_SCHEMA_NAME_PREFIX = DEFAULT_SCHEMA_NAME + "_";
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMetadataScanner.class);

    private static final String NATURAL = "$natural";
    private static final BsonInt32 FORWARD = new BsonInt32(1);
    private static final BsonInt32 REVERSE = new BsonInt32(-1);
    private static final String RANDOM = "$sample";

    /**
     * Create a {@link JsonRoot} for a view model of the collections in the target database.
     * @param properties the connection properties.
     * @return a {@link JsonRoot} with the view model set.
     */
    public static JsonRoot createViewModel(final DocumentDbConnectionProperties properties)
            throws SQLException {

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
            for (String collectionName : collectionNames) {
                final MongoCollection<BsonDocument> collection = database.getCollection(
                        collectionName, BsonDocument.class);
                final Iterator<BsonDocument> cursor = getIterator(properties, collection);
                final DocumentDbCollectionMetadata metadata = DocumentDbCollectionMetadata
                        .create(collectionName, cursor);
                if (metadata != null) {
                    addJsonViewsToSchema(metadata, mapSchema, customSchema.name);
                }
            }
        }
        rootModel.schemas.add(mapSchema);
        return rootModel;
    }

    @VisibleForTesting
    protected static Iterator<BsonDocument> getIterator(
            final DocumentDbConnectionProperties properties,
            final MongoCollection<BsonDocument> collection) throws SQLException {
        final int scanLimit = properties.getMetadataScanLimit();
        final DocumentDbMetadataScanMethod method = properties.getMetadataScanMethod();
        switch (method) {
            case ALL:
                return collection.find().cursor();
            case NATURAL:
                return collection.find().hint(new BsonDocument(NATURAL, FORWARD)).limit(scanLimit).cursor();
            case NATURAL_REVERSE:
                return collection.find().hint(new BsonDocument(NATURAL, REVERSE)).limit(scanLimit).cursor();
            case RANDOM:
                final List<BsonDocument> aggregations = new ArrayList<>();
                aggregations.add(new BsonDocument(RANDOM, new BsonDocument("size", new BsonInt32(scanLimit))));
                return collection.aggregate(aggregations).cursor();
        }
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_FAILURE,
                SqlError.UNSUPPORTED_PROPERTY,
                method.getName()
        );
    }

    @VisibleForTesting
    protected static void addJsonViewsToSchema(final DocumentDbCollectionMetadata metadata,
            final JsonMapSchema schema, final String customSchemaName) {
        for (Map.Entry<String, DocumentDbMetadataTable> table: metadata.getTables().entrySet()) {
            final JsonView tableView = new JsonView();
            tableView.name = table.getKey();

            final String viewTemplate = "select %s from \"%s\".\"%s\"";
            final StringBuilder columnBuilder = new StringBuilder();

            for (Entry<String, DocumentDbMetadataColumn> column :
                    table.getValue().getColumns().entrySet()) {
                appendColumnMap(
                        columnBuilder,
                        column.getKey(),
                        column.getValue().getName(),
                        column.getValue().getSqlType());
            }
            tableView.sql = String.format(
                    viewTemplate, columnBuilder.toString(), customSchemaName, tableView.name);
            schema.tables.add(tableView);
        }
    }

    private static void appendColumnMap(final StringBuilder columnBuilder, final String columnPath, final String columnName,
                                        final int sqlType) {
        final boolean addComma = columnBuilder.length() != 0;
        final String columnTemplate = "cast(_MAP['%s'] AS %s) AS \"%s\"";
        final String columnWithPrecisionTemplate = "cast(_MAP['%s'] AS %s(%s)) AS \"%s\"";
        if (addComma) {
            columnBuilder.append(", ");
        }
        final SqlTypeName sqlTypeName = sqlType == 0
                ? SqlTypeName.NULL
                : SqlTypeName.getNameForJdbcType(sqlType);
        switch (sqlTypeName) {
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INTEGER:
            case TIMESTAMP:
                columnBuilder.append(String.format(columnTemplate,
                        columnPath, sqlTypeName.getName(), columnName));
                break;
            case VARBINARY:
                columnBuilder.append(String.format(columnWithPrecisionTemplate,
                        columnPath, "VARBINARY", Integer.MAX_VALUE, columnName));
                break;
            case VARCHAR:
            case NULL:
                columnBuilder.append(String.format(columnWithPrecisionTemplate,
                        columnPath, "VARCHAR", 0x10000, columnName));
                break;
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
