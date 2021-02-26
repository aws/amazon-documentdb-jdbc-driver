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

import com.mongodb.client.MongoCursor;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.bson.Document;
import org.bson.types.Binary;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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
    static Function1<Document, Object> singletonGetter(final String fieldPath,
            final Class fieldClass,
            final DocumentDbMetadataTable tableMetadata) {
        // DocumentDB: modified - start
        final DocumentDbMetadataColumn column = tableMetadata.getColumnsByPath().get(fieldPath);
        return a0 -> getField(a0, column, fieldPath, fieldClass);
        // DocumentDB: modified - end
    }

    /** Returns a function that projects fields.
     *
     * @param fields List of fields to project; or null to return map
     */
    static Function1<Document, Object[]> listGetter(
            final List<Entry<String, Class>> fields,
            final DocumentDbMetadataTable tableMetadata) {
        return a0 -> {
            // DocumentDB: modified - start
            return fields
                    .stream()
                    .map(field -> {
                        final String path = field.getKey();
                        final DocumentDbMetadataColumn column = tableMetadata.getColumnsByPath().get(path);
                        return getField(a0, column, path, field.getValue());
                    })
                    .toArray();
            // DocumentDB: modified - end
        };
    }

    @SuppressWarnings("unchecked")
    static Function1<Document, Object> getter(
            final List<Entry<String, Class>> fields,
            final DocumentDbMetadataTable tableMetadata) {
        //noinspection unchecked
        // DocumentDB: modified - start
        return fields == null
                ? (Function1) mapGetter()
                : fields.size() == 1
                        ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue(),
                                tableMetadata)
                        : (Function1) listGetter(fields, tableMetadata);
        // DocumentDB: modified - end
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
        if (sourceObject instanceof Document) {
            return ((Document) sourceObject).toJson();
        }
        if (sourceObject instanceof List) {
          return ((List<?>) sourceObject)
              .stream()
                  .map(o1 -> o1 instanceof Document ? ((Document) o1).toJson() : o1)
                  .collect(Collectors.toList());
        }
        // DocumentDB: modified - end
        return sourceObject;
    }

    private static Object getField(
            final Document a0,
            final DocumentDbMetadataColumn column,
            final String path,
            final Class<?> fieldClass)
            throws UnsupportedOperationException {
        if (column == null) {
            throw new UnsupportedOperationException(
                    String.format("Unable to find column metadata for path: %s", path));
        }
        final String[] segmentedPath = path.split("\\.");
        int j = 0;
        Object segmentValue = a0.get(segmentedPath[j]);
        while (segmentValue instanceof Document) {
            final Document document = (Document) segmentValue;
            j++;
            if (j >= segmentedPath.length) {
                break;
            }
            segmentValue = document.get(segmentedPath[j]);
        }
        return convert(segmentValue, fieldClass);
    }
}
