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

import org.apache.calcite.linq4j.Enumerator;

/** Implements the enumerator interface but does not return data. */
class DocumentDbEnumerator implements Enumerator<Object> {

    /** Creates a DocumentDbEnumerator. */
    DocumentDbEnumerator() { }

    @Override public Object current() {
        return null;
    }

    @Override public boolean moveNext() {
        return false;
    }

    @Override public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override public void close() { }
}
