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

package software.amazon.documentdb.jdbc.common.helpers;

import org.junit.jupiter.api.Assertions;
import software.amazon.documentdb.jdbc.common.utilities.Warning;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.atomic.AtomicReference;

public class HelperFunctions {
    public static final String TEST_WARNING_REASON_1 = "warning_1";
    public static final String TEST_WARNING_REASON_2 = "warning_2";
    public static final String TEST_WARNING_UNSUPPORTED = "unsupported";
    public static final String TEST_WARNING_STATE = "state";
    public static final SQLWarning TEST_SQL_WARNING_UNSUPPORTED =
            new SQLWarning(Warning.lookup(Warning.UNSUPPORTED_PROPERTY, TEST_WARNING_UNSUPPORTED));

    /**
     * Simple interface to pass to functions below.
     * @param <R> Template type.
     */
    public interface VerifyValueInterface<R> {
        /**
         * Function to execute.
         * @return Template type.
         * @throws SQLException Exception thrown.
         */
        R function() throws SQLException;
    }

    /**
     * Simple interface to pass to functions below.
     */
    public interface VerifyThrowInterface {
        /**
         * Function to execute.
         * @throws SQLException Exception thrown.
         */
        void function() throws SQLException;
    }

    /**
     * Function to verify that function passed in throws an exception.
     * @param f function to check.
     */
    public static void expectFunctionThrows(final VerifyThrowInterface f) {
        Assertions.assertThrows(SQLException.class, f::function);
    }

    /**
     * Verifies function passed throws the type exception.
     * @param type - the type of expected exception.
     * @param f - the function to execute.
     * @param <T> - the Throwable type expected.
     */
    public  static <T extends Throwable> void expectFunctionThrows(final Class<T> type, final VerifyThrowInterface f) {
        Assertions.assertThrows(type, f::function);
    }

    /**
     * Function to verify that function passed in doesn't throw an exception.
     * @param f function to check.
     */
    public static void expectFunctionDoesntThrow(final VerifyThrowInterface f) {
        Assertions.assertDoesNotThrow(f::function);
    }

    /**
     * Function to verify that function passed in doesn't throw an exception and has correct output value.
     * @param f function to check.
     * @param expected expected value.
     */
    public static void expectFunctionDoesntThrow(final VerifyValueInterface<?> f, final Object expected) {
        final AtomicReference<Object> actual = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> actual.set(f.function()));
        if (actual.get() instanceof SQLWarning) {
            SQLWarning actualWarning = (SQLWarning) actual.get();
            SQLWarning expectedWarning = (SQLWarning) expected;
            do {
                Assertions.assertNotNull(actualWarning);
                Assertions.assertEquals(expectedWarning.getMessage(), actualWarning.getMessage());
                actualWarning = actualWarning.getNextWarning();
                expectedWarning = expectedWarning.getNextWarning();
                // Dummy is used because end points to itself infinitely.
                // Make sure we don't see same warning multiple times in a row.
            } while (expectedWarning != null);
        } else {
            Assertions.assertEquals(expected, actual.get());
        }
    }

    public static SQLWarning getNewWarning1() {
        return new SQLWarning(TEST_WARNING_REASON_1, TEST_WARNING_STATE);
    }

    public static SQLWarning getNewWarning2() {
        return new SQLWarning(TEST_WARNING_REASON_2, TEST_WARNING_STATE);
    }
}
