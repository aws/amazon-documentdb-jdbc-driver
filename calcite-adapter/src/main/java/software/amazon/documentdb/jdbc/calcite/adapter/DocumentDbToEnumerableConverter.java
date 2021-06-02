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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbRel.Implementor;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;
import java.util.AbstractList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
public class DocumentDbToEnumerableConverter
        extends ConverterImpl
        implements EnumerableRel {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbToEnumerableConverter.class);

    protected DocumentDbToEnumerableConverter(
            final RelOptCluster cluster,
            final RelTraitSet traits,
            final RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return new DocumentDbToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    @Override public @Nullable RelOptCost computeSelfCost(final RelOptPlanner planner,
            final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(DocumentDbRules.ENUMERABLE_COST_FACTOR);
    }

    @Override public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        // Generates a call to "find" or "aggregate", depending upon whether
        // an aggregate is present.
        //
        //   ((MongoTable) schema.getTable("zips")).find(
        //     "{state: 'CA'}",
        //     "{city: 1, zipcode: 1}")
        //
        //   ((MongoTable) schema.getTable("zips")).aggregate(
        //     "{$filter: {state: 'CA'}}",
        //     "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: "$pop"}}")
        final BlockBuilder list = new BlockBuilder();
        final DocumentDbRel.Implementor mongoImplementor =
                new DocumentDbRel.Implementor(getCluster().getRexBuilder());
        mongoImplementor.visitChild(0, getInput());
        final RelDataType rowType = getRowType();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), rowType,
                        pref.prefer(JavaRowFormat.ARRAY));
        final Expression fields =
                list.append("fields",
                        constantArrayList(
                                Pair.zip(DocumentDbRules.mongoFieldNames(rowType,
                                        mongoImplementor.getMetadataTable()),
                                        new AbstractList<Class>() {
                                            @Override public Class get(final int index) {
                                                return physType.fieldClass(index);
                                            }

                                            @Override public int size() {
                                                return rowType.getFieldCount();
                                            }
                                        }),
                                Pair.class));
        final Expression paths =
                list.append("paths",
                    constantArrayList(
                            DocumentDbRules.mongoFieldNames(rowType, mongoImplementor.getMetadataTable()),
                            String.class));
        final Expression table =
                list.append("table",
                        mongoImplementor.getTable().getExpression(
                                DocumentDbTable.DocumentDbQueryable.class));
        // DocumentDB: modified - start
        handleVirtualTable(mongoImplementor);
        // DocumentDB: modified - end
        final List<String> opList = Pair.right(mongoImplementor.getList());
        final Expression ops =
                list.append("ops",
                        constantArrayList(opList, String.class));
        final Expression enumerable =
                list.append("enumerable",
                        Expressions.call(table,
                                DocumentDbMethod.MONGO_QUERYABLE_AGGREGATE.getMethod(), fields, paths, ops));
        if (CalciteSystemProperty.DEBUG.value()) {
            LOGGER.info("Mongo: " + opList);
        }
        Hook.QUERY_PLAN.run(opList);
        list.add(
                Expressions.return_(null, enumerable));
        return implementor.result(physType, list.toBlock());
    }

    /** E.g. {@code constantArrayList("x", "y")} returns
     * "Arrays.asList('x', 'y')".
     *
     * @param values List of values
     * @param clazz Type of values
     * @return expression
     */
    private static <T> MethodCallExpression constantArrayList(final List<T> values,
            final Class clazz) {
        return Expressions.call(
                BuiltInMethod.ARRAYS_AS_LIST.method,
                Expressions.newArrayInit(clazz, constantList(values)));
    }

    /** E.g. {@code constantList("x", "y")} returns
     * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
    private static <T> List<Expression> constantList(final List<T> values) {
        return Util.transform(values, Expressions::constant);
    }

    /**
     * Adds aggregation stage to handle virtual tables that may be null.
     * @param implementor the implementor.
     */
    public static void handleVirtualTable(final Implementor implementor) {
        final DocumentDbSchemaTable tableMetadata = implementor.getMetadataTable();
        // Add a match operation if it is a virtual table to remove null rows.
        if (!implementor.isNullFiltered() && DocumentDbJoin.isTableVirtual(tableMetadata)) {
            final String matchFilter = DocumentDbJoin
                    .buildFieldsExistMatchFilter(DocumentDbJoin.getFilterColumns(tableMetadata));
            if (matchFilter != null) {
                implementor.add(0, null, matchFilter);
            }
        }
        implementor.setNullFiltered(true);
    }
}
