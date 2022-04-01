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

import org.bson.BsonType;

import java.util.Map;

class DocumentDbTableSchemaGeneratorTest {
    protected static final String COLLECTION_NAME = DocumentDbTableSchemaGeneratorTest.class.getSimpleName();
    private static final boolean DEMO_MODE = false;

    protected boolean producesVirtualTable(final BsonType bsonType, final BsonType nextBsonType) {
        return (bsonType == BsonType.ARRAY && nextBsonType == BsonType.ARRAY)
                || (bsonType == BsonType.DOCUMENT && nextBsonType == BsonType.DOCUMENT)
                || (bsonType == BsonType.NULL
                && (nextBsonType == BsonType.ARRAY || nextBsonType == BsonType.DOCUMENT))
                || (nextBsonType == BsonType.NULL
                && (bsonType == BsonType.ARRAY || bsonType == BsonType.DOCUMENT));
    }

    protected void printMetadataOutput(final Map<String, DocumentDbSchemaTable> model,
                                     final String testName) {
        if (DEMO_MODE) {
            final String nameOfTest = testName != null ? testName : "TEST";
            System.out.printf("Start of %s%n", nameOfTest);
            System.out.println(model.toString());
            System.out.printf("End of %s%n", nameOfTest);
        }
    }

    protected static String getMethodName() {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        final String methodName;
        final int stackDepth = 2;
        if (stackDepth < stackTraceElements.length) {
            methodName = stackTraceElements[stackDepth].getMethodName();
        } else {
            methodName = "";
        }
        return methodName;
    }
}
