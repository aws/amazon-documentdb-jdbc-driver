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

public class DocumentDbSchemaException extends Exception {
    /**
     * Creates a {@link DocumentDbSchemaException}.
     */
    public DocumentDbSchemaException() {
        super("DocumentDB JDBC Driver schema exception.");
    }

    /**
     * Constructs a new {@link DocumentDbSchemaException} with the specified detail message.
     * @param message the detail message (which is saved for later retrieval by the getMessage() method).
     */
    public DocumentDbSchemaException(final String message) {
        super(message);
    }

    /**
     * Constructs a new {@link DocumentDbSchemaException}  with the specified detail message and cause.
     * Note that the detail message associated with cause is not automatically incorporated in
     * this exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method).
     * @param cause the cause (which is saved for later retrieval by the getCause() method). (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public DocumentDbSchemaException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
