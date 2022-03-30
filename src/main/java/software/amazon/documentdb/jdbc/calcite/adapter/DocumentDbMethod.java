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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Builtin methods in the MongoDB adapter.
 */
public enum DocumentDbMethod {
    // TODO: Investigate using find() here for simpler queries.
    //  See: https://github.com/aws/amazon-documentdb-jdbc-driver/issues/240
    MONGO_QUERYABLE_AGGREGATE(DocumentDbTable.DocumentDbQueryable.class, "aggregate",
            List.class, List.class, List.class);

    @SuppressWarnings("ImmutableEnumChecker")
    private final Method method;

    public Method getMethod() {
        return method;
    }

    public static final ImmutableMap<Method, DocumentDbMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, DocumentDbMethod> builder =
                ImmutableMap.builder();
        for (DocumentDbMethod value : DocumentDbMethod.values()) {
            builder.put(value.method, value);
        }
        MAP = builder.build();
    }

    DocumentDbMethod(final Class clazz, final String methodName, final Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
