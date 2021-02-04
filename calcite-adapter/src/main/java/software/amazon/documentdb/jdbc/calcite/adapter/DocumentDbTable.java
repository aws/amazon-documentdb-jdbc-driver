package software.amazon.documentdb.jdbc.calcite.adapter;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class DocumentDbTable extends AbstractQueryableTable
        implements TranslatableTable {

    private final String collectionName;

    protected DocumentDbTable(final String collectionName) {
        super(Object[].class);
        this.collectionName = collectionName;
    }

    @Override public String toString() {
        return "DocumentDbTable {" + collectionName + "}";
    }

    // TODO: Replace with actual type
    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        final RelDataType mapType =
                typeFactory.createMapType(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.ANY), true));
        return typeFactory.builder().add("_MAP", mapType).build();
    }

    @Override public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
            final SchemaPlus schema, final String tableName) {
        return new DocumentDbQueryable<T>(queryProvider, schema, this, tableName);
    }

    @Override public RelNode toRel(
            final RelOptTable.ToRelContext context,
            final RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new DocumentDbTableScan(cluster, cluster.traitSetOf(DocumentDbRel.CONVENTION),
                relOptTable, this, null);
    }

    /** Executes a "find" operation on the underlying collection.
     *
     * <p>For example,
     * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code></p>
     *
     * @param mongoDb MongoDB connection
     * @param filterJson Filter JSON string, or null
     * @param projectJson Project JSON string, or null
     * @param fields List of fields to project; or null to return map
     * @return Enumerator of results
     */
    private Enumerable<Object> find(final MongoDatabase mongoDb, final String filterJson,
            final String projectJson, final List<Entry<String, Class>> fields) {
        final MongoCollection collection =
                mongoDb.getCollection(collectionName);
        final Bson filter =
                filterJson == null ? null : BsonDocument.parse(filterJson);
        final Bson project =
                projectJson == null ? null : BsonDocument.parse(projectJson);
        final Function1<Document, Object> getter = DocumentDbEnumerator.getter(fields);
        return new AbstractEnumerable<Object>() {
            @Override public Enumerator<Object> enumerator() {
                @SuppressWarnings("unchecked") final FindIterable<Document> cursor =
                        collection.find(filter).projection(project);
                return new DocumentDbEnumerator(cursor.iterator(), getter);
            }
        };
    }

    /** Executes an "aggregate" operation on the underlying collection.
     *
     * <p>For example:
     * <code>zipsTable.aggregate(
     * "{$filter: {state: 'OR'}",
     * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
     * </code></p>
     *
     * @param mongoDb MongoDB connection
     * @param fields List of fields to project; or null to return map
     * @param operations One or more JSON strings
     * @return Enumerator of results
     */
    private Enumerable<Object> aggregate(final MongoDatabase mongoDb,
            final List<Entry<String, Class>> fields,
            final List<String> operations) {
        final List<Bson> list = new ArrayList<>();
        for (String operation : operations) {
            list.add(BsonDocument.parse(operation));
        }
        final Function1<Document, Object> getter =
                DocumentDbEnumerator.getter(fields);
        return new AbstractEnumerable<Object>() {
            @Override public Enumerator<Object> enumerator() {
                final Iterator<Document> resultIterator;
                try {
                    resultIterator = mongoDb.getCollection(collectionName)
                            .aggregate(list).iterator();
                } catch (Exception e) {
                    throw new RuntimeException("While running MongoDB query "
                            + Util.toString(operations, "[", ",\n", "]"), e);
                }
                return new DocumentDbEnumerator(resultIterator, getter);
            }
        };
    }

    /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
     * a {@link DocumentDbTable}.
     *
     * @param <T> element type */
    public static class DocumentDbQueryable<T> extends AbstractTableQueryable<T> {
        DocumentDbQueryable(final QueryProvider queryProvider, final SchemaPlus schema,
                final DocumentDbTable table, final String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        @SuppressWarnings("unchecked")
        @Override public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable =
                    (Enumerable<T>) getTable().find(getMongoDb(), null, null, null);
            return enumerable.enumerator();
        }

        private MongoDatabase getMongoDb() {
            return schema.unwrap(DocumentDbSchema.class).getMongoDatabase();
        }

        private DocumentDbTable getTable() {
            return (DocumentDbTable) table;
        }

        /** Called via code-generation.
         *
         * @see DocumentDbMethod#MONGO_QUERYABLE_AGGREGATE
         * @return an enumerable of the aggregate pipeline
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> aggregate(final List<Entry<String, Class>> fields,
                final List<String> operations) {
            return getTable().aggregate(getMongoDb(), fields, operations);
        }

        /** Called via code-generation.
         *
         * @param filterJson Filter document
         * @param projectJson Projection document
         * @param fields List of expected fields (and their types)
         * @return result of mongo query
         *
         * @see DocumentDbMethod#MONGO_QUERYABLE_FIND
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> find(final String filterJson,
                final String projectJson, final List<Entry<String, Class>> fields) {
            return getTable().find(getMongoDb(), filterJson, projectJson, fields);
        }
    }
}
