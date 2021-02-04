package software.amazon.documentdb.jdbc.calcite.adapter;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoDatabase;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import java.util.Map;

/**
 * Provides a schema for DocumentDB
 */
public class DocumentDbSchema extends AbstractSchema {

    private final MongoDatabase mongoDatabase;

    protected DocumentDbSchema(final MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (String collectionName : mongoDatabase.listCollectionNames()) {
            builder.put(collectionName, new DocumentDbTable(collectionName));
        }

        return builder.build();
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        // TODO: Investigate if this will allow JIT schema
        return super.getSubSchemaMap();
    }
}
