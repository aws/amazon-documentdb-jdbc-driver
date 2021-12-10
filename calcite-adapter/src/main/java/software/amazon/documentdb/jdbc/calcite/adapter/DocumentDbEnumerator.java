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

import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerator;
import org.bson.Document;

import java.util.Iterator;

/** Enumerator that reads from a MongoDB collection. */
class DocumentDbEnumerator implements Enumerator<Object> {
    private final Iterator<Document> cursor;

    /** Creates a DocumentDbEnumerator.
     *
     * @param cursor Mongo iterator (usually a {@link com.mongodb.client.MongoCursor})
     */
    DocumentDbEnumerator(final Iterator<Document> cursor) {
        this.cursor = cursor;
    }

    @Override public Object current() {
        return null;
    }

    @Override public boolean moveNext() {
        return false;
    }

    @Override public void reset() {
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    @Override public void close() {
        if (cursor instanceof AutoCloseable) {
            ((AutoCloseable) cursor).close();
        }
        // AggregationOutput implements Iterator but not DBCursor. There is no
        // available close() method -- apparently there is no open resource.
    }
}
