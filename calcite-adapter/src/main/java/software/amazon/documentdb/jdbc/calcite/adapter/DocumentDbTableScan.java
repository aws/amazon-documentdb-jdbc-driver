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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UnwindOptions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.util.List;
import java.util.Map.Entry;

/**
 * Relational expression representing a scan of a MongoDB collection.
 *
 * <p> Additional operations might be applied,
 * using the "find" or "aggregate" methods.</p>
 */
public class DocumentDbTableScan extends TableScan implements DocumentDbRel {
    private final DocumentDbTable mongoTable;
    private final RelDataType projectRowType;
    private final DocumentDbSchemaTable metadataTable;

    /**
     * Creates a DocumentDbTableScan.
     *
     * @param cluster        Cluster
     * @param traitSet       Traits
     * @param table          Table
     * @param mongoTable     MongoDB table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected DocumentDbTableScan(final RelOptCluster cluster, final RelTraitSet traitSet,
            final RelOptTable table, final DocumentDbTable mongoTable, final RelDataType projectRowType,
            final DocumentDbSchemaTable metadataTable) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.mongoTable = mongoTable;
        this.projectRowType = projectRowType;
        this.metadataTable = metadataTable;

        assert mongoTable != null;
        assert getConvention() == DocumentDbRel.CONVENTION;
    }

    @Override public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override public @Nullable RelOptCost computeSelfCost(final RelOptPlanner planner,
            final RelMetadataQuery mq) {
        // scans with a small project list are cheaper
        final float f = projectRowType == null ? 1f
                : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
    }

    @Override public void register(final RelOptPlanner planner) {
        planner.addRule(DocumentDbToEnumerableConverterRule.INSTANCE);
        for (RelOptRule rule : DocumentDbRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override public void implement(final Implementor implementor) {
        implementor.setTable(table);
        implementor.setDocumentDbTable(mongoTable);
        implementor.setMetadataTable(metadataTable);

        // Add an unwind operation for each embedded array to convert to separate rows.
        // Assumes that all queries will use aggregate and not find.
        // Assumes that outermost arrays are added to the list first so pipeline executes correctly.
        for (Entry<String, DocumentDbSchemaColumn> column : metadataTable.getColumnMap().entrySet()) {
            if (column.getValue().isIndex()) {
                final String indexName = column.getKey();
                final UnwindOptions opts = new UnwindOptions();
                String arrayPath = column.getValue().getFieldPath();
                arrayPath = "$" + arrayPath;
                opts.includeArrayIndex(indexName);
                opts.preserveNullAndEmptyArrays(true);
                implementor.add(null, String.valueOf(Aggregates.unwind(arrayPath, opts)));
            }
        }
    }
}
