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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Sort}
 * relational expression in MongoDB.
 */
public class DocumentDbSort extends Sort implements DocumentDbRel {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbSort.class.getName());

    /**
     * Creates a new {@link DocumentDbSort}
     * @param cluster the cluster.
     * @param traitSet the trait set.
     * @param child the child
     * @param collation the collation
     * @param offset the offset node.
     * @param fetch the fetch node.
     */
    public DocumentDbSort(final RelOptCluster cluster, final RelTraitSet traitSet,
            final RelNode child, final RelCollation collation, final RexNode offset, final RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
        assert getConvention() == DocumentDbRel.CONVENTION;
        assert getConvention() == child.getConvention();
    }

    @Override public @Nullable RelOptCost computeSelfCost(final RelOptPlanner planner,
            final RelMetadataQuery mq) {
        final RelOptCost relOptCost = super.computeSelfCost(planner, mq);
        return relOptCost != null
                ? relOptCost.multiplyBy(DocumentDbRules.SORT_COST_FACTOR)
                : null;
    }

    @Override public Sort copy(final RelTraitSet traitSet, final RelNode input,
            final RelCollation newCollation, final RexNode offset, final RexNode fetch) {
        return new DocumentDbSort(getCluster(), traitSet, input, collation, offset,
                fetch);
    }

    @Override public void implement(final Implementor implementor) {
        implementor.visitChild(0, getInput());
        if (!collation.getFieldCollations().isEmpty()) {
            final List<String> keys = new ArrayList<>();
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                // DocumentDB: modified - start
                final List<String> names = DocumentDbRules.mongoFieldNames(getRowType(), implementor.getMetadataTable());
                final String name = names.get(fieldCollation.getFieldIndex());
                keys.add(DocumentDbRules.maybeQuote(name) + ": " + direction(fieldCollation));
                // DocumentDB: modified - end
                if (false) {
                    // TODO: NULLS FIRST and NULLS LAST
                    switch (fieldCollation.nullDirection) {
                        case FIRST:
                            break;
                        case LAST:
                            break;
                        default:
                            break;
                    }
                }
            }
            implementor.add(null,
                    "{$sort: " + Util.toString(keys, "{", ", ", "}") + "}");
        }
        if (offset != null) {
            implementor.add(null,
                    "{$skip: " + ((RexLiteral) offset).getValue() + "}");
        }
        if (fetch != null) {
            implementor.add(null,
                    "{$limit: {$numberLong: \"" + ((RexLiteral) fetch).getValue() + "\"}}");
        }
        LOGGER.info("Created sort and row limit stages of pipeline.");
        LOGGER.debug("Pipeline stages added: {}",
                implementor.getList().stream()
                        .map(c -> c.right)
                        .toArray());
    }

    private static int direction(final RelFieldCollation fieldCollation) {
        switch (fieldCollation.getDirection()) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return -1;
            case ASCENDING:
            case STRICTLY_ASCENDING:
            default:
                return 1;
        }
    }
}
