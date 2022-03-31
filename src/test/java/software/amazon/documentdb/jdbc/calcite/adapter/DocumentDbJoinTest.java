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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class DocumentDbJoinTest {

    @Test
    void validateCollectionKeys() {
        final List<String> leftPrimaryKeys = new ArrayList<>();
        leftPrimaryKeys.add("_id");

        final List<String> rightPrimaryKeys = new ArrayList<>();
        rightPrimaryKeys.add("_id");
        rightPrimaryKeys.add("other__id");

        final List<String> keysUsed = new ArrayList<>();
        keysUsed.add("_id");

        final DocumentDbJoin documentDbJoin = Mockito.mock(DocumentDbJoin.class,Mockito.CALLS_REAL_METHODS);
        Assertions.assertDoesNotThrow(() -> {
            documentDbJoin.validateMinimumPrimaryKeysUsage(keysUsed,leftPrimaryKeys,rightPrimaryKeys);
            documentDbJoin.validateMinimumPrimaryKeysUsage(keysUsed,leftPrimaryKeys,rightPrimaryKeys);
        });
    }

    @Test
    void validateCollectionKeysException() {
        final List<String> leftPrimaryKeys = new ArrayList<>();
        leftPrimaryKeys.add("_id");
        leftPrimaryKeys.add("_other__id");

        final List<String> rightPrimaryKeys = new ArrayList<>();
        rightPrimaryKeys.add("_id");
        rightPrimaryKeys.add("_other__id");
        rightPrimaryKeys.add("another__id");

        final List<String> keysUsed = new ArrayList<>();
        keysUsed.add("_id");

        final DocumentDbJoin documentDbJoin = Mockito.mock(DocumentDbJoin.class,Mockito.CALLS_REAL_METHODS);
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> documentDbJoin.validateMinimumPrimaryKeysUsage(keysUsed, leftPrimaryKeys, rightPrimaryKeys));
    }
}
