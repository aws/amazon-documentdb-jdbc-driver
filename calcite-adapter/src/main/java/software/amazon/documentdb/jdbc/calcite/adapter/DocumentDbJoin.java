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
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Join} in DocumentDb.
 */
public class DocumentDbJoin extends Join implements DocumentDbRel {

    /**
     * Creates a new {@link DocumentDbJoin}
     * @param cluster the cluster.
     * @param traitSet the trait set.
     * @param left the left node.
     * @param right the right node.
     * @param condition the condition.
     * @param joinType the join type.
     */
    public DocumentDbJoin(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final RelNode left,
            final RelNode right,
            final RexNode condition,
            final JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition,
                ImmutableSet.of(), joinType);
        assert getConvention() == DocumentDbRel.CONVENTION;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(
            final RelOptPlanner planner,
            final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(DocumentDbRules.JOIN_COST_FACTOR);
    }

    @Override
    public Join copy(
            final RelTraitSet traitSet,
            final RexNode conditionExpr,
            final RelNode left,
            final RelNode right,
            final JoinRelType joinType,
            final boolean semiJoinDone) {
        return new DocumentDbJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @Override
    public void implement(final Implementor implementor) {
        // Visit all nodes to the left of the join.
        implementor.visitChild(0, getLeft());
        final DocumentDbTable leftTable = implementor.getDocumentDbTable();
        final DocumentDbMetadataTable leftMetadata =  implementor.getMetadataTable();

        // Create a new implementor and visit all nodes to the right of the join.
        // This implementor can contain operations specific to the right.
        final DocumentDbRel.Implementor rightImplementor =
                new DocumentDbRel.Implementor(implementor.getRexBuilder());
        rightImplementor.visitChild(0, getRight());
        final DocumentDbTable rightTable = rightImplementor.getDocumentDbTable();
        final DocumentDbMetadataTable rightMetadata =  rightImplementor.getMetadataTable();

        if (leftTable.getCollectionName().equals(rightTable.getCollectionName())) {
            joinSameCollection(implementor, rightImplementor, leftTable.getCollectionName(), leftMetadata, rightMetadata);
        } else {
            //TODO: Will need to join tables from different collections through $lookup. Joining
            // on the same collection with arbitrary keys could also be potentially supported with lookup.
            throw new IllegalArgumentException(SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, "join on different collections"));

        }
    }

    /**
     * Performs a "join" on tables from the same collection by combining their metadata and
     * filtering out null rows based on join type.
     * This is only applicable for joins where we are only "denormalizing" virtual tables by joining
     * on foreign keys.
     * @param implementor the implementor from the left side of the join. Operations are
     *                    added to the left.
     * @param rightImplementor the implementor from the rifht side of the join.
     * @param collectionName The collection both tables are from.
     * @param left the metadata of the left side of the join.
     * @param right the metadata of the right side of the join.
     */
    private void joinSameCollection(
            final Implementor implementor,
            final Implementor rightImplementor,
            final String collectionName,
            final DocumentDbMetadataTable left,
            final DocumentDbMetadataTable right) {
        validateSameCollectionJoin(left, right);

        // Add remaining operations from the right.
        rightImplementor.getList().forEach(pair -> implementor.add(pair.left, pair.right));

        // Eliminate null (i.e. "unmatched") rows from any virtual tables based on join type.
        // If an inner join, eliminate any null rows from either table.
        // If a left outer join, eliminate the null rows of the left side.
        // If a right outer join, eliminate the null rows of the right side.
        final ImmutableCollection<DocumentDbMetadataColumn> leftFilterColumns = getFilterColumns(left);
        final ImmutableCollection<DocumentDbMetadataColumn> rightFilterColumns = getFilterColumns(right);
        final Supplier<String> leftFilter = () -> buildFieldsExistMatchFilter(leftFilterColumns);
        final Supplier<String> rightFilter = () -> buildFieldsExistMatchFilter(rightFilterColumns);
        final String filterLeft;
        final String filterRight;
        switch (getJoinType()) {
            case INNER:
                filterLeft = leftFilter.get();
                filterRight = rightFilter.get();
                if (filterLeft != null) {
                    implementor.add(null, filterLeft);
                    implementor.setNullFiltered(true);
                }
                if (filterRight != null) {
                    implementor.add(null, filterRight);
                    implementor.setNullFiltered(true);
                }
                break;
            case LEFT:
                filterLeft = leftFilter.get();
                if (filterLeft != null) {
                    implementor.add(null, filterLeft);
                    implementor.setNullFiltered(true);
                }
                break;
            case RIGHT:
                filterRight = rightFilter.get();
                if (filterRight != null) {
                    implementor.add(null, filterRight);
                    implementor.setNullFiltered(true);
                }
                break;
            case FULL:
                // Full join will retain null rows from either side.
                break;
            default:
                //TODO: Figure out if/how we will support semi-join and anti-join.
                throw new IllegalArgumentException(
                        SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, getJoinType().name()));
        }

        final boolean rightIsVirtual = isTableVirtual(right);
        final boolean leftIsVirtual = isTableVirtual(left);

        // Create a new metadata table representing the denormalized form that will be used
        // in later parts of the query. Resolve column naming collisions from the right table.
        Map<String, DocumentDbMetadataColumn> columnMap = new LinkedHashMap<>(left.getColumns());
        final List<String> renames = new ArrayList<>();
        final Set<String> usedKeys = new LinkedHashSet<>(columnMap.keySet());
        for (Entry<String, DocumentDbMetadataColumn> entry : right.getColumns().entrySet()) {
            final String key = entry.getKey();
            if (columnMap.containsKey(key)) {
                final String newKey = SqlValidatorUtil.uniquify(key, usedKeys, SqlValidatorUtil.EXPR_SUGGESTER);
                final DocumentDbMetadataColumn leftColumn = columnMap.get(key);

                // If the columns correspond to the same field, they may have different values depending on join
                // type. Create a new column and add a new field.
                if (entry.getValue().getPath().equals(leftColumn.getPath())) {
                    columnMap.put(newKey, entry.getValue());

                    final DocumentDbMetadataColumn newRightColumn = entry.getValue().toBuilder()
                            .name(newKey)
                            .resolvedPath(newKey)
                            .build();
                    columnMap.put(newKey, newRightColumn);

                    // Handle any column renames
                    handleColumnRename(renames, newKey, entry.getValue().getPath(),
                            rightIsVirtual, rightFilterColumns);
                    handleColumnRename(renames, leftColumn.getPath(), leftColumn.getPath(),
                            leftIsVirtual, leftFilterColumns);
                } else {
                    columnMap.put(newKey, entry.getValue());
                }
            } else {
                columnMap.put(key, entry.getValue());
            }
        }

        if (!renames.isEmpty()) {
            final String newFields = Util.toString(renames, "{", ", ", "}");
            final String aggregateString = "{ $addFields : " + newFields + "}";
            implementor.add(null, aggregateString);
        }

        // Remove virtual tables.
        columnMap = columnMap
                .entrySet()
                .stream()
                .filter(entry -> Strings.isNullOrEmpty(entry.getValue().getVirtualTableName()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        final DocumentDbMetadataTable metadata = DocumentDbMetadataTable
                .builder()
                .name(left.getName())
                .columns(ImmutableMap.copyOf(columnMap))
                .build();

        final DocumentDbTable joinedTable = new DocumentDbTable(collectionName, metadata);
        implementor.setDocumentDbTable(joinedTable);
        implementor.setMetadataTable(metadata);
    }

    /**
     * Gets whether the given table is virtual - whether it contains foreign key columns.
     *
     * @param table the table to test.
     * @return {@code true} if table contains foreign key columns, {@code false}, otherwise.
     */
    static boolean isTableVirtual(final DocumentDbMetadataTable table) {
        return table.getColumns().values().stream()
                .anyMatch(c -> c.getForeignKey() > 0);
    }

    /**
     * Renames columns appropriately for the join. Adds a condition on whether the fields of
     * a virtual table are not null.
     *
     * @param renames the collection of renamed columns.
     * @param newKey the new key (column name) for the column.
     * @param originalPath the original path.
     * @param tableIsVirtual indicator of whether table is virtual.
     * @param filterColumns list of columns to filter.
     */
    private void handleColumnRename(
            final List<String> renames,
            final String newKey,
            final String originalPath,
            final boolean tableIsVirtual,
            final ImmutableCollection<DocumentDbMetadataColumn> filterColumns) {
        // Set the fields to be their original value unless their parent table is null for this row.
        final StringBuilder ifNullBuilder = new StringBuilder();
        final String newPath = (tableIsVirtual && tryBuildIfNullFieldsCondition(
                filterColumns, ifNullBuilder))
                ? "{ $cond : [ " + ifNullBuilder + ", "
                + DocumentDbRules.maybeQuote("$" + originalPath) + ", null ] }"
                : DocumentDbRules.maybeQuote("$" + originalPath);

        renames.add(DocumentDbRules.maybeQuote(newKey) + ": " + newPath);
    }

    /**
     * Gets the list of columns to add to the filter.
     * @param table the table to get the complete list of columns.
     * @return a collection of columns to filter. Can return an empty list.
     */
    static ImmutableList<DocumentDbMetadataColumn> getFilterColumns(
            final DocumentDbMetadataTable table) {
        // We don't need to check for
        // 1. primary keys,
        // 2. foreign keys (from another table)
        // 3. columns that are "virtual" (i.e. arrays, structures)
        return table.getColumns().values().stream()
                .filter(c -> c.getPrimaryKey() == 0
                        && c.getForeignKey() == 0
                        && Strings.isNullOrEmpty(c.getVirtualTableName()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Creates the aggregate step for matching all provided fields.
     * @param columns the columns that represents a field.
     * @return an aggregate step in JSON format if any field exist, otherwise, null.
     */
    static String buildFieldsExistMatchFilter(
            final ImmutableCollection<DocumentDbMetadataColumn> columns) {
        final StringBuilder builder = new StringBuilder();
        if (!tryBuildFieldsExists(columns, builder)) {
            return null;
        }
        builder.insert(0, "{ \"$match\": ");
        builder.append(" }");
        return builder.toString();
    }

    private static boolean tryBuildFieldsExists(
            final ImmutableCollection<DocumentDbMetadataColumn> columns,
            final StringBuilder builder) {
        int columnCount = 0;
        for (DocumentDbMetadataColumn column : columns) {
            if (columnCount != 0) {
                builder.append(", ");
            }
            builder.append("{ ");
            builder.append(DocumentDbRules.maybeQuote(column.getPath()));
            builder.append(": { \"$exists\": true } }");
            columnCount++;
        }

        if (columnCount == 0) {
            return false;
        }

        if (columnCount > 1) {
            builder.insert(0, "{ \"$or\": [ ");
            builder.append(" ] }");
        }
        return true;
    }

    private static boolean tryBuildIfNullFieldsCondition(
            final ImmutableCollection<DocumentDbMetadataColumn> columns,
            final StringBuilder builder) {
        int columnCount = 0;
        for (DocumentDbMetadataColumn column : columns) {
            if (columnCount != 0) {
                builder.append(", ");
            }
            builder.append("{ $ifNull: [ ");
            builder.append(DocumentDbRules.maybeQuote("$" + column.getPath()));
            builder.append(", false ] }");
            columnCount++;
        }

        if (columnCount == 0) {
            return false;
        }

        if (columnCount > 1) {
            builder.insert(0, "{ \"$or\": [ ");
            builder.append(" ] }");
        }
        return true;
    }

    /**
     * Validates that the same collection join is only denormalizing any virtual tables
     * by checking the join keys and join conditions.
     * @param left the metadata of the left side of the join.
     * @param right the metadata of the right side of the join.
     */
    private void validateSameCollectionJoin(
            final DocumentDbMetadataTable left, final DocumentDbMetadataTable right) {
        // Extract the join keys.
        // We can ignore filterNulls for this case as primary and foreign keys are not nullable.
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<RexNode> nonEquiList = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(
                getLeft(), getRight(), getCondition(), leftKeys, rightKeys, filterNulls, nonEquiList);

        // Check that there are only equality conditions.
        if (!nonEquiList.isEmpty()) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.EQUIJOINS_ON_FK_ONLY));
        }

        // Check that all equality conditions are actually comparing the same fields.
        final List<String> leftNames = DocumentDbRules.mongoFieldNames(getLeft().getRowType(), left, true);
        final List<String> rightNames = DocumentDbRules.mongoFieldNames(getRight().getRowType(), right, true);
        final List<String> leftKeyNames =
                leftKeys.stream().map(leftNames::get).collect(Collectors.toList());
        final List<String> rightKeyNames =
                rightKeys.stream().map(rightNames::get).collect(Collectors.toList());
        if (!leftKeyNames.equals(rightKeyNames)) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.EQUIJOINS_ON_FK_ONLY));
        }

        // Check that if joining with a virtual table, only its complete set of foreign keys are used.
        final List<String> requiredKeys = Streams
                .concat(left.getForeignKeys().stream(), right.getForeignKeys().stream())
                .map(DocumentDbMetadataColumn::getPath)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        Collections.sort(leftKeyNames);
        if (!(leftKeyNames).equals(requiredKeys)) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.EQUIJOINS_ON_FK_ONLY));
        }
    }
}

