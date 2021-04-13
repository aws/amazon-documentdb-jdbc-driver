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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a {@link Filter}
 * relational expression in MongoDB.
 */
public class DocumentDbFilter extends Filter implements DocumentDbRel {

    /**
     * Creates a new {@link DocumentDbFilter}
     * @param cluster the relational option cluster.
     * @param traitSet the trait set.
     * @param child the child.
     * @param condition the condition.
     */
    public DocumentDbFilter(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final RelNode child,
            final RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }

    @Override public @Nullable RelOptCost computeSelfCost(final RelOptPlanner planner,
            final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(DocumentDbRules.FILTER_COST_FACTOR);
    }

    @Override public DocumentDbFilter copy(final RelTraitSet traitSet, final RelNode input,
            final RexNode condition) {
        return new DocumentDbFilter(getCluster(), traitSet, input, condition);
    }

    @Override public void implement(final Implementor implementor) {
        implementor.visitChild(0, getInput());
        // DocumentDB: modified - start
        final DocumentDbRel.Implementor mongoImplementor =
                new DocumentDbRel.Implementor(implementor.getRexBuilder());
        mongoImplementor.visitChild(0, getInput());
        final Translator translator =
                new Translator(implementor.getRexBuilder(),
                        DocumentDbRules.mongoFieldNames(getRowType(),
                                mongoImplementor.getMetadataTable()));
        // DocumentDB: modified - end
        final String match = translator.translateMatch(condition);
        implementor.add(null, match);
    }

    /** Translates {@link RexNode} expressions into MongoDB expression strings. */
    static class Translator {
        private final JsonBuilder builder = new JsonBuilder();
        private final Multimap<String, Pair<String, RexLiteral>> multiMap =
                HashMultimap.create();
        private final Map<String, RexLiteral> eqMap =
                new LinkedHashMap<>();
        private final RexBuilder rexBuilder;
        private final List<String> fieldNames;

        Translator(final RexBuilder rexBuilder, final List<String> fieldNames) {
            this.rexBuilder = rexBuilder;
            this.fieldNames = fieldNames;
        }

        private String translateMatch(final RexNode condition) {
            final Map<String, Object> map = builder.map();
            map.put("$match", translateOr(condition));
            return builder.toJsonString(map);
        }

        private Object translateOr(final RexNode condition) {
            final RexNode condition2 =
                    RexUtil.expandSearch(rexBuilder, null, condition);

            final List<Object> list = new ArrayList<>();
            for (RexNode node : RelOptUtil.disjunctions(condition2)) {
                list.add(translateAnd(node));
            }
            switch (list.size()) {
                case 1:
                    return list.get(0);
                default:
                    final Map<String, Object> map = builder.map();
                    map.put("$or", list);
                    return map;
            }
        }

        /** Translates a condition that may be an AND of other conditions. Gathers
         * together conditions that apply to the same field. */
        private Map<String, Object> translateAnd(final RexNode node0) {
            eqMap.clear();
            multiMap.clear();
            for (RexNode node : RelOptUtil.conjunctions(node0)) {
                translateMatch2(node);
            }
            final Map<String, Object> map = builder.map();
            for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
                multiMap.removeAll(entry.getKey());
                map.put(entry.getKey(), literalValue(entry.getValue()));
            }
            for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
                    : multiMap.asMap().entrySet()) {
                final Map<String, Object> map2 = builder.map();
                for (Pair<String, RexLiteral> s : entry.getValue()) {
                    addPredicate(map2, s.left, literalValue(s.right));
                }
                map.put(entry.getKey(), map2);
            }
            return map;
        }

        private static void addPredicate(final Map<String, Object> map, final String op,
                final Object v) {
            if (map.containsKey(op) && stronger(op, map.get(op), v)) {
                return;
            }
            map.put(op, v);
        }

        /** Returns whether {@code v0} is a stronger value for operator {@code key}
         * than {@code v1}.
         *
         * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
         * "&lt; 100" is a more powerful condition than "&lt; 200".
         */
        private static boolean stronger(final String key, final Object v0, final Object v1) {
            if ("$lt".equals(key) || "$lte".equals(key)) {
                if (v0 instanceof Number && v1 instanceof Number) {
                    return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
                }
                if (v0 instanceof String && v1 instanceof String) {
                    return v0.toString().compareTo(v1.toString()) < 0;
                }
            }
            if ("$gt".equals(key) || "$gte".equals(key)) {
                return stronger("$lt", v1, v0);
            }
            return false;
        }

        private static Object literalValue(final RexLiteral literal) {
            return literal.getValue2();
        }

        private Void translateMatch2(final RexNode node) {
            switch (node.getKind()) {
                case EQUALS:
                    return translateBinary(null, null, (RexCall) node);
                case LESS_THAN:
                    return translateBinary("$lt", "$gt", (RexCall) node);
                case LESS_THAN_OR_EQUAL:
                    return translateBinary("$lte", "$gte", (RexCall) node);
                case NOT_EQUALS:
                    return translateBinary("$ne", "$ne", (RexCall) node);
                case GREATER_THAN:
                    return translateBinary("$gt", "$lt", (RexCall) node);
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary("$gte", "$lte", (RexCall) node);
                default:
                    throw new AssertionError("cannot translate " + node);
            }
        }

        /** Translates a call to a binary operator, reversing arguments if
         * necessary. */
        private Void translateBinary(final String op, final String rop, final RexCall call) {
            final RexNode left = call.operands.get(0);
            final RexNode right = call.operands.get(1);
            boolean b = translateBinary2(op, left, right);
            if (b) {
                return null;
            }
            b = translateBinary2(rop, right, left);
            if (b) {
                return null;
            }
            throw new AssertionError("cannot translate op " + op + " call " + call);
        }

        /** Translates a call to a binary operator. Returns whether successful. */
        private boolean translateBinary2(final String op, final RexNode left, final RexNode right) {
            switch (right.getKind()) {
                case LITERAL:
                    break;
                default:
                    return false;
            }
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch (left.getKind()) {
                case INPUT_REF:
                    final RexInputRef left1 = (RexInputRef) left;
                    final String name = fieldNames.get(left1.getIndex());
                    translateOp2(op, name, rightLiteral);
                    return true;
                case CAST:
                    return translateBinary2(op, ((RexCall) left).operands.get(0), right);
                case ITEM:
                    final String itemName = DocumentDbRules.isItem((RexCall) left);
                    if (itemName != null) {
                        translateOp2(op, itemName, rightLiteral);
                        return true;
                    }
                    // fall through
                default:
                    return false;
            }
        }

        private void translateOp2(final String op, final String name, final RexLiteral right) {
            if (op == null) {
                // E.g.: {deptno: 100}
                eqMap.put(name, right);
            } else {
                // E.g. {deptno: {$lt: 100}}
                // which may later be combined with other conditions:
                // E.g. {deptno: [$lt: 100, $gt: 50]}
                multiMap.put(name, Pair.of(op, right));
            }
        }
    }
}
