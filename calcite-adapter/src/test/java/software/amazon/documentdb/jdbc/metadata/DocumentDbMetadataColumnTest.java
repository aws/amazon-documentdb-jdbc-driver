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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

public class DocumentDbMetadataColumnTest {

    @DisplayName("Tests equals() method with different combinations.")
    @Test
    void testEquals() {
        final DocumentDbMetadataColumn column1 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column2 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column3 = new DocumentDbMetadataColumn(
                1, 2, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column4 = new DocumentDbMetadataColumn(
                1, 1, 2, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column5 = new DocumentDbMetadataColumn(
                1, 1, 1, 2, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column6 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "other",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column7 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "other", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column8 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "other", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column9 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", true, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column10 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "other", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column11 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "other",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column12 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.VARCHAR, BsonType.INT64, false, true, "table", "table");
        final DocumentDbMetadataColumn column13 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.STRING, false, true, "table", "table");
        final DocumentDbMetadataColumn column14 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, true, true, "table", "table");
        final DocumentDbMetadataColumn column15 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, false, "table", "table");
        final DocumentDbMetadataColumn column16 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "other", "table");
        final DocumentDbMetadataColumn column17 = new DocumentDbMetadataColumn(
                1, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "other");
        final DocumentDbMetadataColumn column18 = new DocumentDbMetadataColumn(
                2, 1, 1, 1, "table",
                "table", "path", false, "path", "column",
                JdbcType.BIGINT, BsonType.INT64, false, true, "table", "table");

        Assertions.assertTrue(column1.equals(column1));
        Assertions.assertTrue(column1.equals(column2));
        Assertions.assertFalse(column1.equals(column3));
        Assertions.assertFalse(column1.equals(column4));
        Assertions.assertFalse(column1.equals(column5));
        Assertions.assertFalse(column1.equals(column6));
        Assertions.assertFalse(column1.equals(column7));
        Assertions.assertFalse(column1.equals(column8));
        Assertions.assertFalse(column1.equals(column9));
        Assertions.assertFalse(column1.equals(column10));
        Assertions.assertFalse(column1.equals(column11));
        Assertions.assertFalse(column1.equals(column12));
        Assertions.assertFalse(column1.equals(column13));
        Assertions.assertFalse(column1.equals(column14));
        Assertions.assertFalse(column1.equals(column15));
        Assertions.assertFalse(column1.equals(column16));
        Assertions.assertFalse(column1.equals(column17));
        Assertions.assertFalse(column1.equals(column18));
        Assertions.assertFalse(column1.equals(new Object()));
    }
}
