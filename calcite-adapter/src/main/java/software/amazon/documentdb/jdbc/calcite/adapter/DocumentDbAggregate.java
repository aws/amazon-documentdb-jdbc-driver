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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of
 * {@link Aggregate} relational expression
 * in MongoDB.
 */
public class DocumentDbAggregate
        extends Aggregate
        implements DocumentDbRel {

    /**
     * Creates a new {@link DocumentDbAggregate}
     * @param cluster the {@link RelOptCluster} cluster.
     * @param traitSet the trait set.
     * @param input the node input.
     * @param groupSet the group set.
     * @param groupSets the group sets.
     * @param aggCalls the aggregate calls.
     * @throws InvalidRelException if aggregate call includes the unsupported DISTINCT
     */
    public DocumentDbAggregate(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final RelNode input,
            final ImmutableBitSet groupSet,
            final List<ImmutableBitSet> groupSets,
            final List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
        assert getConvention() == CONVENTION;
        assert getConvention() == input.getConvention();

        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.isDistinct()) {
                throw new InvalidRelException(
                        "distinct aggregation not supported");
            }
        }
        switch (getGroupType()) {
            case SIMPLE:
                break;
            default:
                throw new InvalidRelException("unsupported group type: "
                        + getGroupType());
        }
    }

    /**
     * DEPRECATED
     * @param cluster the cluster.
     * @param traitSet the trait set.
     * @param input the input.
     * @param indicator the indicator.
     * @param groupSet the group set.
     * @param groupSets the group sets.
     * @param aggCalls the aggregate calls.
     * @throws InvalidRelException if aggregate call includes the unsupported DISTINCT
     */
    @Deprecated // to be removed before 2.0
    public DocumentDbAggregate(final RelOptCluster cluster, final RelTraitSet traitSet,
            final RelNode input, final boolean indicator, final ImmutableBitSet groupSet,
            final List<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls)
            throws InvalidRelException {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
        checkIndicator(indicator);
    }

    @Override public Aggregate copy(final RelTraitSet traitSet, final RelNode input,
            final ImmutableBitSet groupSet, final List<ImmutableBitSet> groupSets,
            final List<AggregateCall> aggCalls) {
        try {
            return new DocumentDbAggregate(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override public void implement(final Implementor implementor) {
        implementor.visitChild(0, getInput());
        final List<String> list = new ArrayList<>();
        final List<String> inNames =
                DocumentDbRules.mongoFieldNames(getInput().getRowType());
        final List<String> outNames = DocumentDbRules.mongoFieldNames(getRowType());
        int i = 0;
        if (groupSet.cardinality() == 1) {
            final String inName = inNames.get(groupSet.nth(0));
            list.add("_id: " + DocumentDbRules.maybeQuote("$" + inName));
            ++i;
        } else {
            final List<String> keys = new ArrayList<>();
            for (int group : groupSet) {
                final String inName = inNames.get(group);
                keys.add(inName + ": " + DocumentDbRules.quote("$" + inName));
                ++i;
            }
            list.add("_id: " + Util.toString(keys, "{", ", ", "}"));
        }
        for (AggregateCall aggCall : aggCalls) {
            list.add(
                    DocumentDbRules.maybeQuote(outNames.get(i++)) + ": "
                            + toMongo(aggCall.getAggregation(), inNames, aggCall.getArgList()));
        }
        implementor.add(null,
                "{$group: " + Util.toString(list, "{", ", ", "}") + "}");
        final List<String> fixups;
        if (groupSet.cardinality() == 1) {
            fixups = new AbstractList<String>() {
                @Override public String get(final int index) {
                    final String outName = outNames.get(index);
                    return DocumentDbRules.maybeQuote(outName) + ": "
                            + DocumentDbRules.maybeQuote("$" + (index == 0 ? "_id" : outName));
                }

                @Override public int size() {
                    return outNames.size();
                }
            };
        } else {
            fixups = new ArrayList<>();
            fixups.add("_id: 0");
            i = 0;
            for (int group : groupSet) {
                fixups.add(
                        DocumentDbRules.maybeQuote(outNames.get(group))
                                + ": "
                                + DocumentDbRules.maybeQuote("$_id." + outNames.get(group)));
                ++i;
            }
            for (AggregateCall ignored : aggCalls) {
                final String outName = outNames.get(i++);
                fixups.add(
                        DocumentDbRules.maybeQuote(outName) + ": " + DocumentDbRules.maybeQuote(
                                "$" + outName));
            }
        }
        if (!groupSet.isEmpty()) {
            implementor.add(null,
                    "{$project: " + Util.toString(fixups, "{", ", ", "}") + "}");
        }
    }

    private static String toMongo(final SqlAggFunction aggregation, final List<String> inNames,
            final List<Integer> args) {
        if (aggregation == SqlStdOperatorTable.COUNT) {
            if (args.size() == 0) {
                return "{$sum: 1}";
            } else {
                assert args.size() == 1;
                final String inName = inNames.get(args.get(0));
                return "{$sum: {$cond: [ {$eq: ["
                        + DocumentDbRules.quote(inName)
                        + ", null]}, 0, 1]}}";
            }
        } else if (aggregation instanceof SqlSumAggFunction
                || aggregation instanceof SqlSumEmptyIsZeroAggFunction) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "{$sum: " + DocumentDbRules.maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.MIN) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "{$min: " + DocumentDbRules.maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.MAX) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "{$max: " + DocumentDbRules.maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.AVG) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "{$avg: " + DocumentDbRules.maybeQuote("$" + inName) + "}";
        } else {
            throw new AssertionError("unknown aggregate " + aggregation);
        }
    }
}
