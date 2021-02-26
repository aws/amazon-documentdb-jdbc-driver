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
import com.mongodb.client.MongoCollection;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonRoot;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
import java.util.Map.Entry;

/**
 * Provides a way to scan metadata in DocumentDB collections
 */
public class DocumentDbMetadataScanner {
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
    public static JsonRoot createViewModel(final DocumentDbConnectionProperties properties) {

        final JsonRoot rootModel = new JsonRoot();
        rootModel.version = "1.0";
        rootModel.defaultSchema = properties.getDatabase();
        final JsonCustomSchema customSchema = new JsonCustomSchema();
        customSchema.name = properties.getDatabase();
        customSchema.factory = DocumentDbSchemaFactory.class.getName();
        customSchema.operand = new HashMap<>();

        // Copy properties into the "operand" which is passed to the custom Schema factory.
        for (Entry<Object, Object> entry : properties.entrySet()) {
            customSchema.operand.put(
                    String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        rootModel.schemas.add(customSchema);
        return rootModel;
    }

    /**
     * Gets an iterator for the requested scan type.
     *
     * @param properties the connection properties including scan type and limit.
     * @param collection the {@link MongoCollection} to scan.
     * @return an {@link Iterator} for the documents.
     * @throws SQLException if unsupported scan type provided.
     */
    @VisibleForTesting
    public static Iterator<BsonDocument> getIterator(
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
}
