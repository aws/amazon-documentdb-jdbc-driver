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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGeneratorHelper.toName;

/**
 * These tests check that table and column names are truncated to fit the max identifier length
 * for Calcite queries.
 */
public class DocumentDbTableSchemaGeneratorIdentifierTest extends DocumentDbTableSchemaGeneratorTest {
    @Test
    @DisplayName("Tests identifier names that are longer than allowed maximum")
    void testLongName() {
        String testPath = "a.b.c";
        final Map<String, String> tableNameMap = new HashMap<>();
        String testName;

        testName = toName(testPath, tableNameMap, 128);
        Assertions.assertEquals("a_b_c", testName);

        testName = toName(testPath, tableNameMap, 4);
        Assertions.assertEquals("a_c", testName);

        // Uses cached value
        testName = toName(testPath, tableNameMap, 128);
        Assertions.assertEquals("a_c", testName);

        testPath = "a.b.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_g", testName);

        testPath = "a.c.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_1", testName);

        testPath = "a.d.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_2", testName);

        testPath = "a.e.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_3", testName);

        testPath = "a.f.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_4", testName);

        testPath = "a.g.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_5", testName);

        testPath = "a.h.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_6", testName);

        testPath = "a.i.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_7", testName);

        testPath = "a.j.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_8", testName);

        testPath = "a.k.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f_9", testName);

        testPath = "a.l.c.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("a_d_e_f10", testName);

        testPath = "12345678901.x.y.d.e.f.g";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("_d_e_f_g", testName);

        testPath = "baseTable01"; // "12345678901";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("baseTable0", testName);

        testPath = "baseTable01.childtble01";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("hildtble01", testName);

        testPath = "baseTable02.childtble01";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("hildtble02", testName);

        testPath = "baseTable02.childtble02";
        testName = toName(testPath, tableNameMap, 10);
        Assertions.assertEquals("hildtble03", testName);
    }

    @DisplayName("Tests that even deeply nested documents and array have name length less than max.")
    @Test
    void testDeeplyNestedDocumentsArraysForSqlNameLength() {
        BsonValue doc = new BsonNull();
        for (int i = 199; i >= 0; i--) {
            doc = new BsonDocument("_id", new BsonInt32(i))
                    .append(i + "field", new BsonInt32(i))
                    .append(i + "doc", doc)
                    .append(i + "array", new BsonArray(Collections.singletonList(new BsonInt32(i))));
        }
        final Map<String, DocumentDbSchemaTable> tableMap = DocumentDbTableSchemaGenerator
                .generate(COLLECTION_NAME, Collections.singleton((BsonDocument) doc).iterator());

        Assertions.assertEquals(400, tableMap.size());
        tableMap.keySet().stream()
                .map(tableName -> tableName.length() <= DEFAULT_IDENTIFIER_MAX_LENGTH)
                .forEach(Assertions::assertTrue);
        tableMap.values().stream()
                .flatMap(schemaTable -> schemaTable.getColumns().stream())
                .map(schemaColumn -> schemaColumn.getSqlName().length()
                        <= DEFAULT_IDENTIFIER_MAX_LENGTH)
                .forEach(Assertions::assertTrue);
    }
}
