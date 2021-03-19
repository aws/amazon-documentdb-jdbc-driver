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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A starting point for implementing JOIN but needs A LOT of additional work.
 * Calcite was handling the joins for us after getting cursors to both tables
 * but we would ideally like to do the joins purely in MQL in 1 query.
 */
public class DocumentDbJoin extends EnumerableMergeJoin {

    protected DocumentDbJoin(
            final RelOptCluster cluster,
            final RelTraitSet traits,
            final RelNode left,
            final RelNode right,
            final RexNode condition,
            final Set<CorrelationId> variablesSet,
            final JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
    }

    @Override
    public DocumentDbJoin copy(
            final RelTraitSet traitSet,
            final RexNode condition,
            final RelNode left,
            final RelNode right,
            final JoinRelType joinType,
            final boolean semiJoinDone) {
        return new DocumentDbJoin(
                getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        final Expression leftExpression =
                builder.append("left", leftResult.block);
        final Result rightResult =
                implementor.visitChild(this, 1, (EnumerableRel) right, pref);
        final Expression rightExpression =
                builder.append("right", rightResult.block);
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final PhysType physType =
                PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray());


        final Expression enumerable =
                builder.append("enumerable",
                        Expressions.call(
                                DocumentDbMethod.MONGO_JOIN.getMethod(), leftExpression, rightExpression));

        builder.add(
                Expressions.return_(null, enumerable));
        return implementor.result(physType, builder.toBlock());
    }

    /**
     * Called with code generation.
     * This is not a proper implementation and only "works" for a natural inner join between 2 tables
     * belonging to the same collection.
     * Need to figure out:
     *      - How to extract the join condition and type.
     *      - How to handle tables from different collections.
     *      - How to actually "join" 2 or MORE tables in MongoDB using an aggregation stage.
     * @param left an enumerator for the left side of the join
     * @param right an enumerator for the right side of the join
     * @return a new enumerable to iterate through the "joined" tables
     */
    public static Enumerable<Object> innerJoin(final Enumerable<?> left, final Enumerable<?> right) {
        final DocumentDbEnumerable leftTable = (DocumentDbEnumerable) left;
        final DocumentDbEnumerable rightTable = (DocumentDbEnumerable) right;

        // Check if the belong to the same collection
        if (leftTable.getCollectionName().equals(rightTable.getCollectionName())) {
            // Join the column maps and remove virtual table columns
            Map<String, DocumentDbMetadataColumn> columnMap = new HashMap<>(leftTable.getMetadataTable().getColumns());
            columnMap.putAll(rightTable.getMetadataTable().getColumns());
            columnMap = columnMap
                    .entrySet()
                    .stream()
                    .filter(entry -> Strings.isNullOrEmpty(entry.getValue().getVirtualTableName()))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

            final DocumentDbMetadataTable metadata = DocumentDbMetadataTable
                    .builder()
                    .path(leftTable.getMetadataTable().getPath())
                    .name(leftTable.getMetadataTable().getName())
                    .columns(ImmutableMap.copyOf(columnMap))
                    .build();

            // Join the field lists
            final List<Entry<String, Class>> fields = new ArrayList<>();
            fields.addAll(leftTable.getFields());
            fields.addAll(rightTable.getFields());

            // Join the operations
            final List<String> operations = new ArrayList<>();
            operations.addAll(leftTable.getOperations());
            operations.addAll(rightTable.getOperations());

            // Process the join conditions


            final DocumentDbTable joinedTable = new DocumentDbTable(leftTable.getCollectionName(), metadata);
            return joinedTable.aggregate(leftTable.getMongoDb(), fields, operations);
        }

        // If the tables are from a different collection, we need to join them using $lookup.
        return null;
    }
}

