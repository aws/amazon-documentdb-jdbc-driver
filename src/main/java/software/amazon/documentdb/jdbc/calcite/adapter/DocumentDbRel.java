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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.util.Pair;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that uses Mongo calling convention.
 */
public interface DocumentDbRel extends RelNode {

    /**
     * Implements the implementor.
     * @param implementor the implementor to implement
     */
    void implement(Implementor implementor);

    /** Calling convention for relational operations that occur in MongoDB. */
    Convention CONVENTION = new Convention.Impl("MONGO", DocumentDbRel.class);

    /** Callback for the implementation process that converts a tree of
     * {@link DocumentDbRel} nodes into a MongoDB query. */
    class Implementor {

        // DocumentDB: modified - start
        private List<Pair<String, String>> list = new ArrayList<>();
        private final RexBuilder rexBuilder;
        private RelOptTable table;
        private DocumentDbSchemaTable metadataTable;
        private DocumentDbTable documentDbTable;
        private final List<String> unwinds = new ArrayList<>();
        private final List<String> collisionResolutions = new ArrayList<>();
        private String virtualTableFilter;
        private boolean nullFiltered = false;
        private boolean join = false;
        private boolean resolutionNeedsUnwind = false;
        private final Instant currentTime = Instant.now();

        // DocumentDB: modified - end

        public List<Pair<String, String>> getList() {
            return list;
        }

        public void setList(final List<Pair<String, String>> list) {
            this.list = list;
        }

        public RexBuilder getRexBuilder() {
            return rexBuilder;
        }

        public RelOptTable getTable() {
            return table;
        }

        public void setTable(final RelOptTable table) {
            this.table = table;
        }

        // DocumentDB: modified - start
        public void setDocumentDbTable(final DocumentDbTable table) {
            this.documentDbTable = table;
        }

        public DocumentDbTable getDocumentDbTable() {
            return documentDbTable;
        }

        public DocumentDbSchemaTable getMetadataTable() {
            return metadataTable;
        }

        public void setMetadataTable(final DocumentDbSchemaTable metadataTable) {
            this.metadataTable = metadataTable;
        }

        public Implementor(final RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        public void add(final String findOp, final String aggOp) {
            list.add(Pair.of(findOp, aggOp));
        }

        public void add(final int index, final String findOp, final String aggOp) {
            list.add(index, Pair.of(findOp, aggOp));
        }

        public void addUnwind(final String op) {
            unwinds.add(op);

        }

        public List<String> getUnwinds() {
            return unwinds;
        }

        public void setVirtualTableFilter(final String op) {
            this.virtualTableFilter = op;
        }

        public String getVirtualTableFilter() {
            return virtualTableFilter;
        }

        public void addCollisionResolution(final String op) {
            collisionResolutions.add(op);
        }

        public List<String> getCollisionResolutions() {
            return collisionResolutions;
        }

        public void setResolutionNeedsUnwind(final boolean resolutionNeedsUnwind) {
            this.resolutionNeedsUnwind = resolutionNeedsUnwind;
        }

        public boolean isResolutionNeedsUnwind() {
            return resolutionNeedsUnwind;
        }

        public boolean isNullFiltered() {
            return nullFiltered;
        }

        public void setNullFiltered(final boolean nullFiltered) {
            this.nullFiltered = nullFiltered;
        }

        public boolean isJoin() {
            return join;
        }

        public void setJoin(final boolean join) {
            this.join = join;
        }
        // DocumentDB: modified - end

        public void visitChild(final int ordinal, final RelNode input) {
            assert ordinal == 0;
            final boolean isJoin = isJoin();
            ((DocumentDbRel) input).implement(this);
            setJoin(isJoin);
        }

        public Instant getCurrentTime() {
            return currentTime;
        }
    }
}
