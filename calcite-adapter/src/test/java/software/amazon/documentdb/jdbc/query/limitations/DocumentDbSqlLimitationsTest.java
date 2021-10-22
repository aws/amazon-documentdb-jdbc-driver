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

package software.amazon.documentdb.jdbc.query.limitations;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingServiceTest;

import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbSqlLimitationsTest extends DocumentDbQueryMappingServiceTest {
    private static final String COLLECTION_NAME = "testCollection";
    private static final String OTHER_COLLECTION_NAME = "otherTestCollection";
    private static DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void initialize() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ "
                                + "{ \"field\" : 1, \"field1\": \"value\" }, "
                                + "{ \"field\" : 2, \"field2\" : \"value\" , \"field3\" : { \"field4\": 3} } ]}");
        final BsonDocument otherDocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key1\", \"otherArray\" : [ { \"field\" : 1, \"field3\": \"value\" }, { \"field\" : 2, \"field3\" : \"value\" } ]}");
        insertBsonDocuments(COLLECTION_NAME, new BsonDocument[] {document});
        insertBsonDocuments(OTHER_COLLECTION_NAME, new BsonDocument[] {otherDocument});
        queryMapper = getQueryMappingService();
    }

    @Test
    @DisplayName("Tests that GROUP BY with ROLLUP() fails as this is not supported.")
    void testRollup() {
        // DocumentDBAggregate throws exception when group type is CUBE or ROLLUP because we do not
        // have any logic to handle grouping by multiple group sets. Not sure if we can get the right
        // behaviour with only $group.
        // $facet may be useful here but it is not yet supported in DocumentDB.
        final String query =
                String.format(
                        "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\" GROUP BY ROLLUP( \"%1$s\", \"%2$s\", \"%3$s\")",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1",
                        getDatabaseName(),
                        COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(query),
                "Query requiring ROLLUP() should throw an exception.");
    }

    @Test
    @DisplayName("Tests that RANK() function should fail as it is not supported.")
    void testRank() {
        // Need to implement in RexToMongoTranslator.
        // $setWindowFields and $rank were added in 5.0 to support this. May be difficult to implement
        // with some combination of $group/$merge/$facet.
        // Translation:
        //  DocumentDbProject(EXPR$0=[RANK() OVER (PARTITION BY $3 ORDER BY $2)]):
        //    DocumentDbTableScan(table=[[database, testCollection_array]]):
        final String query =
                String.format(
                        "SELECT RANK() OVER (PARTITION BY \"field1\" ORDER BY \"field\" ASC) FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(query),
                "Query requiring RANK() should throw an exception.");
    }

    @Test
    @DisplayName("Tests that ROUND() function should fail as it is not supported.")
    void testRound() {
        // Need to implement in RexToMongoTranslator.
        // $round was only added in 4.2. May be able to emulate combining some other arithmetic
        // operators.
        final String query =
                String.format(
                        "SELECT ROUND(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(query),
                "Query requiring ROUND() should throw an exception");
    }

    @Test
    @DisplayName(
            "Tests that subqueries in WHERE clause using IN or EXISTS should fail as these are not supported.")
    void testWhereWithSubqueries() {
        // WHERE NOT EXISTS, IN, NOT IN are treated as semi-join or anti-join. They go through the
        // DocumentDbJoin
        // implementation but fail join condition validations.
        // $lookup could be used to support these cases.
        // Translation:
        // DocumentDbToEnumerableConverter: ...
        //  DocumentDbJoin(condition=[=($3, $9)], joinType=[semi]): ...
        //    DocumentDbTableScan(table=[[database, testCollection_array]]): ...
        //    DocumentDbTableScan(table=[[database, testCollection_array]]): ...
        final String subqueryWithIn =
                String.format(
                        "SELECT * FROM \"%1$s\".\"%2$s\" WHERE \"field1\" "
                                + "IN (SELECT \"field2\" FROM \"%1$s\".\"%2$s\")",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(subqueryWithIn),
                "Query with IN and a subquery should throw an exception.");
        // Translation:
        // DocumentDbToEnumerableConverter: ...
        //  DocumentDbJoin(condition=[=($2, $7)], joinType=[semi]): ...
        //      DocumentDbTableScan(table=[[database, testCollection_array]]): ...
        //      DocumentDbFilter(condition=[IS NOT NULL($2)]): ...
        //          DocumentDbTableScan(table=[[database, otherTestCollection_otherArray]]): ...
        final String subqueryWithExists =
                String.format(
                        "SELECT * FROM \"%1$s\".\"%2$s\" WHERE EXISTS "
                                + "(SELECT * FROM \"%1$s\".\"%3$s\" WHERE \"%2$s\".\"field\" = \"%3$s\".field)",
                        getDatabaseName(), COLLECTION_NAME + "_array", OTHER_COLLECTION_NAME + "_otherArray");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(subqueryWithExists),
                "Query with EXISTS and a subquery should throw an exception.");
        // Translation:
        // DocumentDbToEnumerableConverter: ...
        //  DocumentDbProject(testCollection__id=[$0], array_index_lvl_0=[$1], field=[$2], field1=[$3],
        // field2=[$4]): ...
        //   DocumentDbJoin(condition=[<($2, $5)], joinType=[inner]): ...
        //      DocumentDbTableScan(table=[[database, testCollection_array]]): ...
        //      DocumentDbAggregate(group=[{}], EXPR$0=[MAX($2)]): ...
        //          DocumentDbTableScan(table=[[database, testCollection_array]]): ...
        final String subqueryWithSingleValue =
                String.format(
                        "SELECT * FROM \"%1$s\".\"%2$s\" WHERE \"field\" "
                                + "< (SELECT MAX(\"field\") FROM \"%1$s\".\"%2$s\")",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(subqueryWithSingleValue),
                "Query with comparison operator and a subquery should throw an exception.");
    }

    @Test
    @DisplayName(
            "Tests that set operations UNION, INTERSECT and EXCEPT should fail as these are not supported.")
    void testSetOperations() {
        // No rule to transform the LogicalUnion.
        // Same collection only - may be able to combine $facet(unsupported) + $setUnion
        // Generic - $unionWith (4.4)
        final String unionQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" UNION SELECT \"%s\" FROM \"%s\".\"%s\"",
                        getDatabaseName(),
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        getDatabaseName(),
                        COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(unionQuery),
                "Query requiring UNION should throw an exception.");
        // No rule to transform the LogicalIntersect.
        // Same collection only - may be able to combine $facet(unsupported) + $setIntersection
        final String intersectQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" INTERSECT SELECT \"%s\" FROM \"%s\".\"%s\"",
                        getDatabaseName(),
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        getDatabaseName(),
                        COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(intersectQuery),
                "Query requiring INTERSECT should throw an exception.");
        // No rule to transform the LogicalMinus.
        // Same collection only - may be able to combine $facet(unsupported) + $setDifference
        final String exceptQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" EXCEPT SELECT \"%s\" FROM \"%s\".\"%s\"",
                        getDatabaseName(),
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        getDatabaseName(),
                        COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(exceptQuery),
                "Query requiring EXCEPT or MINUS should throw an exception.");
    }

    @Test
    @DisplayName(
            "Tests that subqueries in the SELECT clause using IN or EXISTS should fail "
                    + "as these are not supported.")
    void testSelectWithSubqueries() {
        // The various subquery aggregates are determined first and then added as a left outer join to
        // the table.
        // This would also be supported by $lookup. Other uses of subqueries in the SELECT clause should
        // be similar.
        // Translation:
        // DocumentDbProject(EXPR$0=[CASE(=($1, $0), $2, $3)]):
        //    DocumentDbJoin(condition=[true], joinType=[left]):
        //      DocumentDbJoin(condition=[true], joinType=[left]):
        //        DocumentDbJoin(condition=[true], joinType=[left]):
        //          DocumentDbProject(field=[$2]):
        //            DocumentDbTableScan(table=[[database, testCollection_array]]):
        //          DocumentDbAggregate(group=[{}], EXPR$0=[MAX($2)]):
        //            DocumentDbTableScan(table=[[database, testCollection_array]]):
        //        DocumentDbAggregate(group=[{}], EXPR$0=[AVG($2)]):
        //          DocumentDbTableScan(table=[[database, testCollection_array]]):
        //      DocumentDbAggregate(group=[{}], EXPR$0=[MIN($2)]):
        //        DocumentDbTableScan(table=[[database, testCollection_array]]): `
        final String singleValueSubquery =
                String.format(
                        "SELECT CASE WHEN (SELECT MAX(\"field\") FROM \"%1$s\".\"%2$s\") = \"field\""
                                + "THEN (SELECT AVG(\"field\") FROM \"%1$s\".\"%2$s\") "
                                + "ELSE (SELECT MIN(\"field\") FROM \"%1$s\".\"%2$s\") END "
                                + "FROM \"%1$s\".\"%2$s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertThrows(
                SQLException.class,
                () -> queryMapper.get(singleValueSubquery),
                "Query requiring scalar subquery should throw an exception.");

        // A scalar subquery used like this should only return a single value. Calcite wraps potentially
        // non-single value subqueries in a SINGLE_VALUE aggregate function.
        // The SINGLE_VALUE function should return the value if there is only one. Otherwise, it should
        // error out at runtime.
        // This behaviour may be hard to push-down. Additionally, it has same challenges as above.
        // Translation:
        // DocumentDbToEnumerableConverter:
        //  DocumentDbProject(EXPR$0=[CASE(=($0, $1), 'yes':VARCHAR(3), 'no':VARCHAR(3))]):
        //    DocumentDbJoin(condition=[true], joinType=[left]):
        //      DocumentDbProject(field1=[$3]):
        //        DocumentDbTableScan(table=[[database, testCollection_array]]):
        //      DocumentDbAggregate(group=[{}], agg#0=[SINGLE_VALUE($4)]):
        //        DocumentDbTableScan(table=[[database, testCollection_array]]): r
        final String multipleValueSubQuery =
                String.format(
                        "SELECT CASE WHEN \"field1\" = (SELECT \"field2\" FROM \"%1$s\".\"%2$s\")"
                                + "THEN 'yes' ELSE 'no' END "
                                + "FROM \"%1$s\".\"%2$s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate SINGLE_VALUE",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(multipleValueSubQuery))
                        .getMessage(),
                "Query requiring SINGLE_VALUE function should throw an exception.");
    }

    @Test
    @DisplayName(
            "Tests that STDDEV(), STDEDEV_POP(), STD_DEV_SAMP(), VAR_POP and VAR_SAMP() should fail "
                    + "as these are not supported aggregate functions.")
    void testUnsupportedAggregateFunctions() {
        // $stdDevPop and $stdDevSamp are in 3.6 onwards but are not supported in DocumentDB.
        // Variance can be derived from $stdDevPop and $stdDevSamp (variance = stdDev ^2).
        // $covariancePop and $covarianceSamp were added in 5.0.
        final String stddev =
                String.format(
                        "SELECT STDDEV(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate STDDEV",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(stddev)).getMessage(),
                "Query requiring STDDEV should throw an exception.");
        final String stddevPop =
                String.format(
                        "SELECT STDDEV_POP(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate STDDEV_POP",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(stddevPop))
                        .getMessage(),
                "Query requiring STDDEV_POP should throw an exception.");
        final String stddevSamp =
                String.format(
                        "SELECT STDDEV_SAMP(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate STDDEV_SAMP",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(stddevSamp))
                        .getMessage(),
                "Query requiring STDDEV_SAMP should throw an exception.");
        final String varPop =
                String.format(
                        "SELECT VAR_POP(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate VAR_POP",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(varPop)).getMessage(),
                "Query requiring VAR_POP should throw an exception.");
        final String varSamp =
                String.format(
                        "SELECT VAR_SAMP(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        Assertions.assertEquals(
                "unknown aggregate VAR_SAMP",
                Assertions.assertThrows(AssertionError.class, () -> queryMapper.get(varSamp)).getMessage(),
                "Query requiring VAR_SAMP should throw an exception.");
    }
}
