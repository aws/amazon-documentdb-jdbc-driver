package software.amazon.documentdb.jdbc.calcite.adapter;

import com.mongodb.client.MongoCursor;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.bson.Document;
import org.bson.types.Binary;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** Enumerator that reads from a MongoDB collection. */
class DocumentDbEnumerator implements Enumerator<Object> {
    private final Iterator<Document> cursor;
    private final Function1<Document, Object> getter;
    private Object current;

    /** Creates a DocumentDbEnumerator.
     *
     * @param cursor Mongo iterator (usually a {@link com.mongodb.client.MongoCursor})
     * @param getter Converts an object into a list of fields
     */
    DocumentDbEnumerator(final Iterator<Document> cursor,
            final Function1<Document, Object> getter) {
        this.cursor = cursor;
        this.getter = getter;
    }

    @Override public Object current() {
        return current;
    }

    @Override public boolean moveNext() {
        try {
            if (cursor.hasNext()) {
                final Document map = cursor.next();
                current = getter.apply(map);
                return true;
            } else {
                current = null;
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override public void close() {
        if (cursor instanceof MongoCursor) {
            ((MongoCursor) cursor).close();
        }
        // AggregationOutput implements Iterator but not DBCursor. There is no
        // available close() method -- apparently there is no open resource.
    }

    static Function1<Document, Map> mapGetter() {
        return a0 -> (Map) a0;
    }

    /** Returns a function that projects a single field. */
    static Function1<Document, Object> singletonGetter(final String fieldName,
            final Class fieldClass) {
        return a0 -> convert(a0.get(fieldName), fieldClass);
    }

    /** Returns a function that projects fields.
     *
     * @param fields List of fields to project; or null to return map
     */
    static Function1<Document, Object[]> listGetter(
            final List<Entry<String, Class>> fields) {
        return a0 -> {
            final Object[] objects = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                final Entry<String, Class> field = fields.get(i);
                final String name = field.getKey();
                objects[i] = convert(a0.get(name), field.getValue());
            }
            return objects;
        };
    }

    @SuppressWarnings("unchecked")
    static Function1<Document, Object> getter(
            final List<Entry<String, Class>> fields) {
        //noinspection unchecked
        return fields == null
                ? (Function1) mapGetter()
                : fields.size() == 1
                        ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue())
                        : (Function1) listGetter(fields);
    }

    @SuppressWarnings("JdkObsolete")
    private static Object convert(final Object o, final Class clazz) {
        // DocumentDB: modified - start
        Object sourceObject = o;
        // DocumentDB: modified - end
        Class sourceClazz = clazz;
        if (sourceObject == null) {
            return null;
        }
        Primitive primitive = Primitive.of(sourceClazz);
        if (primitive != null) {
            sourceClazz = primitive.boxClass;
        } else {
            primitive = Primitive.ofBox(sourceClazz);
        }
        if (sourceClazz.isInstance(sourceObject)) {
            return sourceObject;
        }
        if (sourceObject instanceof Date && primitive != null) {
            // DocumentDB: modified - begin
            sourceObject = ((Date) sourceObject).getTime();
            // DocumentDB: modified - end
        }
        if (sourceObject instanceof Number && primitive != null) {
            return primitive.number((Number) sourceObject);
        }
        // DocumentDB: modified - begin
        if (sourceObject instanceof Binary) {
            return ((Binary) sourceObject).getData();
        }
        // DocumentDB: modified - end
        return sourceObject;
    }
}
