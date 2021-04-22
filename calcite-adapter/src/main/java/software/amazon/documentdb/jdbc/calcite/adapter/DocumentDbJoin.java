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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UnwindOptions;
import lombok.SneakyThrows;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbCollectionMetadata;
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
import java.util.stream.Stream;

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
            joinSameCollection(
                    implementor,
                    rightImplementor,
                    leftTable.getCollectionName(),
                    leftMetadata,
                    rightMetadata);
        } else {
            joinDifferentCollections(
                    implementor,
                    rightImplementor,
                    leftTable.getCollectionName(),
                    rightTable.getCollectionName(),
                    leftMetadata,
                    rightMetadata);
        }
    }

    /**
     * Performs a "join" on tables from the same collection by combining their metadata and
     * filtering out null rows based on join type.
     * This is only applicable for joins where we are only "denormalizing" virtual tables by joining
     * on foreign keys.
     * @param implementor the implementor from the left side of the join. Operations are
     *                    added to the left.
     * @param rightImplementor the implementor from the right side of the join.
     * @param collectionName The collection both tables are from.
     * @param leftTable the metadata of the left side of the join.
     * @param rightTable the metadata of the right side of the join.
     */
    private void joinSameCollection(
            final Implementor implementor,
            final Implementor rightImplementor,
            final String collectionName,
            final DocumentDbMetadataTable leftTable,
            final DocumentDbMetadataTable rightTable) {
        validateSameCollectionJoin(leftTable, rightTable);

        // Add remaining operations from the right.
        rightImplementor.getList().forEach(pair -> implementor.add(pair.left, pair.right));

        // Eliminate null (i.e. "unmatched") rows from any virtual tables based on join type.
        // If an inner join, eliminate any null rows from either table.
        // If a left outer join, eliminate the null rows of the left side.
        final ImmutableCollection<DocumentDbMetadataColumn> leftFilterColumns = getFilterColumns(leftTable);
        final ImmutableCollection<DocumentDbMetadataColumn> rightFilterColumns = getFilterColumns(rightTable);
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
                }
                if (filterRight != null) {
                    implementor.add(null, filterRight);
                }
                implementor.setNullFiltered(true);
                break;
            case LEFT:
                filterLeft = leftFilter.get();
                if (filterLeft != null) {
                    implementor.add(null, filterLeft);
                }
                implementor.setNullFiltered(true);
                break;
            default:
                throw new IllegalArgumentException(
                        SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, getJoinType().name()));
        }

        final boolean rightIsVirtual = isTableVirtual(rightTable);
        final boolean leftIsVirtual = isTableVirtual(leftTable);

        // Filter out unneeded columns from the left and right sides.
        final Map<String, DocumentDbMetadataColumn> leftColumns = getRequiredColumns(leftTable, this::getLeft);
        final Map<String, DocumentDbMetadataColumn> rightColumns = getRequiredColumns(rightTable, this::getRight);

        // Create a new metadata table representing the denormalized form that will be used
        // in later parts of the query. Resolve column naming collisions from the right table.
        final Map<String, DocumentDbMetadataColumn> columnMap = new LinkedHashMap<>(leftColumns);
        final List<String> renames = new ArrayList<>();
        final Set<String> usedKeys = new LinkedHashSet<>(columnMap.keySet());
        for (Entry<String, DocumentDbMetadataColumn> entry : rightColumns.entrySet()) {
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

        final DocumentDbMetadataTable metadata = DocumentDbMetadataTable
                .builder()
                .name(leftTable.getName())
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
                        && Strings.isNullOrEmpty(c.getVirtualTableName())
                        && !c.isGenerated())
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

    /**
     * Performs a "join" on tables from the different collections using a $lookup stage.
     * @param implementor the implementor from the left side of the join. Operations are
     *                    added to the left.
     * @param rightImplementor the implementor from the right side of the join.
     * @param leftCollectionName the name of the collection of the left table.
     * @param rightCollectionName the name of the collection of the right table.
     * @param leftTable the metadata of the left side of the join.
     * @param rightTable the metadata of the right side of the join.
     */
    @SneakyThrows
    private void joinDifferentCollections(
            final Implementor implementor,
            final Implementor rightImplementor,
            final String leftCollectionName,
            final String rightCollectionName,
            final DocumentDbMetadataTable leftTable,
            final DocumentDbMetadataTable rightTable)  {
        // Remove null rows from the left and right, if any.
        DocumentDbToEnumerableConverter.handleVirtualTable(implementor);
        DocumentDbToEnumerableConverter.handleVirtualTable(rightImplementor);

        // Validate that this is a simple equality join.
        validateDifferentCollectionJoin();

        // Determine the new field in the joined documents that will hold the matched rows from the right.
        final String rightMatches = rightTable.getName();

        // Filter out unneeded columns from the left and right sides.
        final Map<String, DocumentDbMetadataColumn> leftColumns = getRequiredColumns(leftTable, this::getLeft);
        final Map<String, DocumentDbMetadataColumn> rightColumns = getRequiredColumns(rightTable, this::getRight);

        // Determine the new metadata. Handle any naming collisions from the right side. Columns
        // from the right will now be nested under field specified by rightMatches.
        final Map<String, DocumentDbMetadataColumn> columnMap = new LinkedHashMap<>(leftColumns);
        final Set<String> usedKeys = new LinkedHashSet<>(columnMap.keySet());
        for (Entry<String, DocumentDbMetadataColumn> entry : rightColumns.entrySet()) {
            final String key = SqlValidatorUtil.uniquify(entry.getKey(), usedKeys, SqlValidatorUtil.EXPR_SUGGESTER);
            final DocumentDbMetadataColumn newColumn = entry.getValue().toBuilder()
                    .resolvedPath(DocumentDbCollectionMetadata.combinePath(rightMatches,entry.getValue().getPath()))
                    .build();
            columnMap.put(key, newColumn);
        }
        final DocumentDbMetadataTable metadata = DocumentDbMetadataTable
                .builder()
                .name(leftCollectionName)
                .columns(ImmutableMap.copyOf(columnMap))
                .build();
        final DocumentDbTable joinedTable = new DocumentDbTable(leftCollectionName, metadata);
        implementor.setDocumentDbTable(joinedTable);
        implementor.setMetadataTable(metadata);

        // Add the lookup stage. This is the stage that "joins" the 2 collections.
        final JsonBuilder jsonBuilder = new JsonBuilder();
        final Map<String, Object> lookupMap = jsonBuilder.map();
        final Map<String, Object> lookupFields = new LinkedHashMap<>();

        // 1. Add collection to join.
        lookupFields.put("from", rightCollectionName);

        // 2. Fields from the left need to be in let so they can be used in $match.
        final Map<String, String> letExpressions =
                leftColumns.values().stream()
                        .collect(
                                Collectors.toMap(
                                        DocumentDbMetadataColumn::getName,
                                        column -> "$" + column.getPath()));
        lookupFields.put("let", letExpressions);

        // 3. Add any stages from the right implementor. Convert the json strings
        // into objects so they can be added as a list to the lookup pipeline.
        final List<Map<String, Object>> stages = new ArrayList<>();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        for (Pair<String, String> operations : rightImplementor.getList()) {
            final String stage = operations.right;
            final Map<String, Object> map = mapper.readValue(stage, new TypeReference<LinkedHashMap<String, Object>>() { });
            stages.add(map);
        }

        // 4. Determine the $match stage for the pipeline. This is the join condition.
        final JoinTranslator translator = new JoinTranslator(implementor.getRexBuilder(), leftColumns, rightColumns);
        stages.add(translator.translateMatch(getCondition()));

        // 5. Add all stages in order to the pipeline.
        lookupFields.put("pipeline", stages);

        // 6. Add the new field where the matches will be placed.
        lookupFields.put("as", rightMatches);

        lookupMap.put("$lookup", lookupFields);
        implementor.add(null, jsonBuilder.toJsonString(lookupMap));

        // Unwind the matched rows. Preserve null/empty arrays (unmatched rows) depending on join type.
        final UnwindOptions opts = new UnwindOptions();
        switch (getJoinType()) {
            case INNER:
                // Remove rows for which there were no matches.
                opts.preserveNullAndEmptyArrays(false);
                break;
            case LEFT:
                // Keep rows for which there were no matches.
                opts.preserveNullAndEmptyArrays(true);
                break;
            default:
                throw new IllegalArgumentException(SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, getJoinType().name()));
        }
        implementor.add(null, String.valueOf(Aggregates.unwind("$" + rightMatches, opts)));
    }

    /**
     * Temporary check to reject joins the translator may not handle correctly.
     */
    private void validateDifferentCollectionJoin() {
        // Extract the join keys.
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<RexNode> nonEquiList = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(
                getLeft(), getRight(), getCondition(), leftKeys, rightKeys, filterNulls, nonEquiList);

        // Check that there is only a single equality condition and no non equality conditions.
        if (!nonEquiList.isEmpty() || leftKeys.size() != 1 || rightKeys.size() != 1) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.SINGLE_EQUIJOIN_ONLY));
        }
    }

    private LinkedHashMap<String, DocumentDbMetadataColumn> getRequiredColumns(
            final DocumentDbMetadataTable table,
            final Supplier<RelNode> getNode) {
        final List<String> fieldNames = getNode.get().getRowType().getFieldNames();
        return table.getColumns().entrySet().stream()
                .filter(entry -> fieldNames.contains(entry.getKey()))
                .collect(Collectors.toMap(
                        Entry::getKey, Entry::getValue,
                        (u, v) -> u, LinkedHashMap::new));
    }

    /**
     * POC of a translator for the join condition.
     * Based on Translator class in DocumentDbFilter. For $lookup, we need to put
     * the match conditions inside $expr so we can reference fields from the left.
     * We also specify the conditions as $gte: [ $field, $$field2 ] rather than field : { $gte: $field2 }
     */
    private static class JoinTranslator {
        private final JsonBuilder builder = new JsonBuilder();
        private final RexBuilder rexBuilder;
        private final List<String> fieldNames;

        JoinTranslator(
                final RexBuilder rexBuilder,
                final Map<String, DocumentDbMetadataColumn> leftColumns,
                final Map<String, DocumentDbMetadataColumn> rightColumns) {
            this.rexBuilder = rexBuilder;

            // The indexes used by RexInputRef nodes follows the order in
            // the output row (getRowType()) which is a concatenation of the 2
            // input row types (getLeft.getRowType() and getRight.getRowType()).
            // But we cannot just use mongoFieldNames with the merged metadata table
            // because the left fields will be referenced by their names as specified in "let"
            // while the right fields will be referenced by their original paths.

            // Left field names use their names as specified in the let field and need "$$"
            final List<String> leftFieldNames =
                    leftColumns.values().stream()
                            .map(column -> "$$" + column.getName())
                            .collect(Collectors.toList());
            // Right field names use their path combined with "$".
            final List<String> rightFieldNames =
                    rightColumns.values().stream()
                            .map(column -> "$" + column.getPath())
                            .collect(Collectors.toList());
            this.fieldNames =
                    Stream.concat(leftFieldNames.stream(), rightFieldNames.stream())
                            .collect(Collectors.toList());
        }

        private Map<String, Object> translateMatch(final RexNode condition) {
            final Map<String, Object> matchMap = builder.map();
            final Map<String, Object> exprMap = builder.map();
            exprMap.put("$expr", translateOr(condition));
            matchMap.put("$match", exprMap);
            return matchMap;
        }

        /** Translates a condition that may be an OR of other conditions. */
        private Object translateOr(final RexNode condition) {
            final RexNode condition2 =
                    RexUtil.expandSearch(rexBuilder, null, condition);

            // Breaks down the condition by ORs.
            final List<Object> list = new ArrayList<>();
            for (RexNode node : RelOptUtil.disjunctions(condition2)) {
                list.add(translateAnd(node));
            }
            if (list.size() == 1) {
                return list.get(0);
            }
            final Map<String, Object> map = builder.map();
            map.put("$or", list);
            return map;
        }

        /** Translates a condition that may be an AND of other conditions. */
        private Object translateAnd(final RexNode node0) {
            // Breaks down the condition by ANDs. But the ANDs may have nested ORs!
            // These will break it.
            final List<Map<String, Object>> list = new ArrayList<>();
            for (RexNode node : RelOptUtil.conjunctions(node0)) {
                list.add(translateMatch2(node));
            }

            if (list.size() == 1) {
                return list.get(0);
            }
            final Map<String, Object> map = builder.map();
            map.put("$and", list);
            return map;
        }

        private Object getValue(final RexNode node) {
            switch (node.getKind()) {
                case INPUT_REF:
                    return fieldNames.get(((RexInputRef) node).getIndex());
                case LITERAL:
                    return ((RexLiteral) node).getValue2();
                default:
                    // Does not handle a node that is CAST or ITEM yet.
                    throw new AssertionError("cannot translate " + node);
            }
        }

        private Map<String, Object> translateMatch2(final RexNode node) {
            switch (node.getKind()) {
                case EQUALS:
                    return translateBinary("$eq", (RexCall) node);
                case LESS_THAN:
                    return translateBinary("$lt", (RexCall) node);
                case LESS_THAN_OR_EQUAL:
                    return translateBinary("$lte", (RexCall) node);
                case NOT_EQUALS:
                    return translateBinary("$ne", (RexCall) node);
                case GREATER_THAN:
                    return translateBinary("$gt", (RexCall) node);
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary("$gte", (RexCall) node);
                default:
                    // Does not handle that the node may be a nested OR node.
                    throw new AssertionError("cannot translate " + node);
            }
        }

        /** Translates a call to a binary operator. */
        private Map<String, Object> translateBinary(final String op, final RexCall call) {
            final Map<String, Object> map = builder.map();
            final Object left = getValue(call.operands.get(0));
            final Object right = getValue(call.operands.get(1));
            final List<Object> items = new ArrayList<>();
            items.add(left);
            items.add(right);
            map.put(op, items);
            return map;
        }
    }
}

