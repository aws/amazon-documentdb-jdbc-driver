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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.util.Pair;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataTable;

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

        private final List<Pair<String, String>> list = new ArrayList<>();
        private final RexBuilder rexBuilder;
        private DocumentDbTable mongoTable;
        private RelOptTable table;
        private DocumentDbMetadataTable metadataTable;

        public List<Pair<String, String>> getList() {
            return list;
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

        public void setMongoTable(final DocumentDbTable mongoTable) {
            this.mongoTable = mongoTable;
        }

        // DocumentDB: modified - start
        public DocumentDbMetadataTable getMetadataTable() {
            return metadataTable;
        }

        public void setMetadataTable(final DocumentDbMetadataTable metadataTable) {
            this.metadataTable = metadataTable;
        }
        // DocumentDB: modified - end

        public Implementor(final RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        public void add(final String findOp, final String aggOp) {
            list.add(Pair.of(findOp, aggOp));
        }

        public void visitChild(final int ordinal, final RelNode input) {
            assert ordinal == 0;
            ((DocumentDbRel) input).implement(this);
        }
    }
}
