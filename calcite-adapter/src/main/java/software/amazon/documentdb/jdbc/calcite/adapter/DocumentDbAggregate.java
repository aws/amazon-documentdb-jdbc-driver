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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbRules.getNormalizedIdentifier;
import static software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbRules.maybeQuote;

/**
 * Implementation of
 * {@link Aggregate} relational expression
 * in MongoDB.
 */
public class DocumentDbAggregate
        extends Aggregate
        implements DocumentDbRel {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DocumentDbAggregate.class.getName());

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
        final DocumentDbRel.Implementor mongoImplementor =
                new DocumentDbRel.Implementor(implementor.getRexBuilder());
        mongoImplementor.visitChild(0, getInput());
        // DocumentDB: modified - start
        final List<String> mongoFieldNames =
                DocumentDbRules.mongoFieldNames(getInput().getRowType(),
                        mongoImplementor.getMetadataTable());
        final List<String> inNames = getInput().getRowType().getFieldNames();
        final List<String> outNames = getRowType().getFieldNames();
        final LinkedHashMap<String, DocumentDbSchemaColumn> columnMap = new LinkedHashMap<>(implementor.getMetadataTable().getColumnMap());

        int columnIndex = 0;
        if (groupSet.cardinality() == 1) {
            final String outName = outNames.get(columnIndex);
            final String inName = inNames.get(groupSet.nth(0));
            final String fieldName = mongoFieldNames.get(groupSet.nth(0));
            final DocumentDbSchemaColumn oldColumn = implementor.getMetadataTable().getColumnMap().get(inName);
            list.add("_id: " + maybeQuote("$" + fieldName));
            columnMap.put(outName, getUpdatedColumn(oldColumn, outName));
            ++columnIndex;
        } else {
            final List<String> keys = new ArrayList<>();
            for (int group : groupSet) {
                final String outName = outNames.get(columnIndex);
                final String inName = inNames.get(group);
                final String fieldName =  mongoFieldNames.get(group);
                final DocumentDbSchemaColumn oldColumn = implementor.getMetadataTable().getColumnMap().get(inName);
                keys.add(maybeQuote(acceptedMongoFieldName(outName)) + ": " + DocumentDbRules.quote("$" + fieldName));
                columnMap.put(outName, getUpdatedColumn(oldColumn, outName));
                ++columnIndex;
            }
            list.add("_id: " + Util.toString(keys, "{", ", ", "}"));
        }

        for (AggregateCall aggCall : aggCalls) {
            final String outName = outNames.get(columnIndex++);
            list.add(
                    maybeQuote(acceptedMongoFieldName(outName)) + ": "
                            + toMongo(aggCall.getAggregation(), mongoFieldNames, aggCall.getArgList(), aggCall.isDistinct()));
            columnMap.put(outName,
                    DocumentDbMetadataColumn.builder()
                            .isGenerated(true)
                            .fieldPath(acceptedMongoFieldName(outName))
                            .sqlName(outName)
                            .build());

        }
        implementor.add(null,
                "{$group: " + Util.toString(list, "{", ", ", "}") + "}");
        final List<String> fixups = getFixups(aggCalls, groupSet, outNames);

        if (!groupSet.isEmpty()
                || aggCalls.stream().anyMatch(aggCall -> aggCall.isDistinct() || aggCall.getAggregation() == SqlStdOperatorTable.SUM)) {
            implementor.add(null,
                    "{$project: " + Util.toString(fixups, "{", ", ", "}") + "}");
        }

        // Set the metadata table with the updated column map.
        final DocumentDbSchemaTable oldMetadata = implementor.getMetadataTable();
        final DocumentDbSchemaTable metadata = DocumentDbMetadataTable.builder()
                .sqlName(oldMetadata.getSqlName())
                .collectionName(oldMetadata.getCollectionName())
                .columns(columnMap)
                .build();
        implementor.setMetadataTable(metadata);
        implementor.setDocumentDbTable(
                new DocumentDbTable(implementor.getDocumentDbTable().getCollectionName(), metadata));
        LOGGER.info("Created aggregation stages of pipeline.");
        LOGGER.debug("Pipeline stages added: {}",
                implementor.getList().stream()
                        .map(c -> c.right)
                        .toArray());
        // DocumentDB: modified - end
    }

    private static DocumentDbSchemaColumn getUpdatedColumn(final DocumentDbSchemaColumn oldColumn, final String outName) {
        return DocumentDbMetadataColumn.builder()
                .fieldPath(oldColumn.getFieldPath())
                .sqlName(oldColumn.getSqlName())
                .sqlType(oldColumn.getSqlType())
                .dbType(oldColumn.getDbType())
                .isIndex(oldColumn.isIndex())
                .isPrimaryKey(oldColumn.isPrimaryKey())
                .foreignKeyTableName(oldColumn.getForeignKeyTableName())
                .foreignKeyColumnName(oldColumn.getForeignKeyColumnName())
                .resolvedPath(acceptedMongoFieldName(outName))
                .build();
    }

    private static String toMongo(final SqlAggFunction aggregation, final List<String> inNames,
            final List<Integer> args, final boolean isDistinct) {

        // Apart from COUNT(*) which has 0 arguments, supported aggregations should be a called with only 1 argument.
        if (!(args.isEmpty() && aggregation == SqlStdOperatorTable.COUNT) && args.size() != 1) {
            throw new AssertionError("aggregate with incorrect number of arguments: " + aggregation);
        }

        // For distinct calls, add to a set and get aggregate after.
        if (isDistinct) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "{$addToSet: " + maybeQuote("$" + inName) + "}";
        }

        if (aggregation == SqlStdOperatorTable.COUNT) {
            if (args.isEmpty()) {
                return "{$sum: 1}";
            } else {
                final String inName = inNames.get(args.get(0));
                return "{$sum: {$cond: [ {$gt: " + "[" + maybeQuote("$" + inName) + ", null]}, 1, 0]}}";
            }
        } else if (aggregation == SqlStdOperatorTable.SUM) {
            final String inName = inNames.get(args.get(0));
            return "{$push: " + maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.SUM0) {
            final String inName = inNames.get(args.get(0));
            return "{$sum: " + maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.MIN) {
            final String inName = inNames.get(args.get(0));
            return "{$min: " + maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.MAX) {
            final String inName = inNames.get(args.get(0));
            return "{$max: " + maybeQuote("$" + inName) + "}";
        } else if (aggregation == SqlStdOperatorTable.AVG) {
            final String inName = inNames.get(args.get(0));
            return "{$avg: " + maybeQuote("$" + inName) + "}";
        } else {
            throw new AssertionError("unknown aggregate " + aggregation);
        }
    }

    private static String acceptedMongoFieldName(final String path) {
        return getNormalizedIdentifier(path).replace('.', '_');
    }

    private static String setToAggregate(final SqlAggFunction aggFunction, final String outName) {
        if (aggFunction == SqlStdOperatorTable.COUNT) {
            // Return size of set with null values removed.
            return "{$size: {$filter: {"
                    + "input:" + maybeQuote("$" + outName) + ", "
                    + "cond: { $gt: [ '$$this', null]}}}}";
        } else if (aggFunction == SqlStdOperatorTable.AVG) {
            return "{$avg: " + maybeQuote("$" + outName) + " }";
        } else if (aggFunction == SqlStdOperatorTable.SUM) {
            return arrayToSum(outName);
        } else if (aggFunction == SqlStdOperatorTable.SUM0) {
            return "{$sum: " + maybeQuote("$" + outName) + " }";
        } else {
            throw new AssertionError("unknown distinct aggregate" + aggFunction);
        }
    }

    private static String arrayToSum(final String outName) {
        // If there are any non-null values, return the sum. Otherwise, return null.
        return String.format(
                "{$cond: [ {$gt: [ {$size: {$filter: {"
                + "input: %1$s, "
                + "cond: { $gt: [ '$$this', null]}}}}, 0]}, "
                + "{$sum:  %1$s }, null]}",
                maybeQuote("$" + outName));
    }

    /**
     * Determines the $project stage after the $group stage. "Fixups" are needed
     * when columns from the group set are selected or there are distinct aggregate calls.
     * Logic was pulled out of original implementation of implement method.
     * @param aggCalls the aggregate calls.
     * @param groupSet the group set.
     * @param outNames the names of the output row type.
     * @return list of fields that should be projected.
     */
    private static List<String> getFixups(
            final List<AggregateCall> aggCalls,
            final ImmutableBitSet groupSet,
            final List<String> outNames) {
        // DocumentDB: modified - start
        final List<String> fixups = new ArrayList<>();
        int columnIndex = 0;
        if (groupSet.cardinality() == 1) {
            fixups.add(maybeQuote(outNames.get(columnIndex++)) + ": " + maybeQuote("$" + "_id"));
        } else {
            fixups.add("_id: 0");
            for (int group : groupSet) {
                final String outName = acceptedMongoFieldName(outNames.get(columnIndex++));
                fixups.add(
                        maybeQuote(outName)
                                + ": "
                                + maybeQuote("$_id." + acceptedMongoFieldName(outName)));
            }

        }
        for (AggregateCall aggCall : aggCalls) {
            final String outName = acceptedMongoFieldName(outNames.get(columnIndex++));
            // Get the aggregate for any sets made in $group stage.
            if (aggCall.isDistinct()) {
                fixups.add(maybeQuote(outName) + ": " + setToAggregate(
                        aggCall.getAggregation(), outName));
            } else if (aggCall.getAggregation() == SqlStdOperatorTable.SUM) {
                // If there are any non-nulls, return the sum. Otherwise, return null.
                fixups.add(maybeQuote(outName) + ": " + arrayToSum(outName));
            } else {
                fixups.add(
                        maybeQuote(outName) + ": " + maybeQuote(
                                "$" + outName));
            }
        }
        return fixups;
        // DocumentDB: modified - end
    }
}
