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
