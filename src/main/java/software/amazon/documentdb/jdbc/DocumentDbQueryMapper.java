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

package software.amazon.documentdb.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalcitePrepare.CalciteSignature;
import org.apache.calcite.jdbc.CalcitePrepare.SparkHandler;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.impl.LongSchemaVersion;
import org.apache.calcite.tools.RelRunner;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbEnumerable;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbSchemaFactory;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseMetadata;
import software.amazon.documentdb.jdbc.metadata.JdbcColumnMetaData;

import java.util.List;

/**
 * This is the POC of our module that maps SQL to MQL.
 */
public class DocumentDbQueryMapper {
    private final DocumentDbPrepareContext prepareContext;
    private final CalcitePrepare prepare;
    private final DocumentDbDatabaseMetadata databaseMetadata;

    /**
     * Holds the CalcitePrepare.Context and the CalcitePrepare generated for a particular connection.
     * The default prepare factory is used like in CalciteConnectImpl.
     * @param properties the connection properties
     */
    public DocumentDbQueryMapper(final DocumentDbConnectionProperties properties,
            final DocumentDbDatabaseMetadata databaseMetadata) {
        this.databaseMetadata = databaseMetadata;
        this.prepareContext = new DocumentDbPrepareContext(
                getRootSchema(databaseMetadata, properties),
                properties.getDatabase(),
                properties);
        this.prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    }

    /**
     * Uses CalcitePrepare API to parse and validate sql and convert to MQL.
     * @param sql the query in sql
     * @return the query context that has the fields, aggregation stages, and table metadata.
     */
    public DocumentDbMqlQueryContext getMqlQueryContext(final String sql) {
        final CalcitePrepare.Query<Object> query = CalcitePrepare.Query.of(sql);
        // In prepareSql:
        // -    We validate the sql based on the schema in prepareContext.
        // -    SqlToRelConverter turns this into the RelNode tree. (SQL->AST)
        // -    The query planner optimizes the tree with the Document DB adapter rules we have.
        // -    We then visit each node (depth first, left-right-root) and go into its implement method.
        //      The implement methods turn the RelNodes into a physical plan. (AST->MQL)
        final CalciteSignature<?> signature = prepare.prepareSql(prepareContext, query, Object[].class, -1);

        // Enumerable contains the operations and fields we need to do the aggregation call.
        // Signature also contains a column list that has information about the columns/types of the
        // return row (ordinal, nullability, precision, etc). This is different than our own DocumentDbColumnMetadata.
        // TODO: We also might want to use additional information from signature to construct the result set.
        final Enumerable<?> enumerable = signature.enumerable(prepareContext.getDataContext());
        if (enumerable instanceof DocumentDbEnumerable) {
            final DocumentDbEnumerable documentDbEnumerable = (DocumentDbEnumerable) enumerable;
            return DocumentDbMqlQueryContext.builder()
                    .columnMetaData(JdbcColumnMetaData.fromCalciteColumnMetaData(signature.columns)) // This is essentially the ResultSetMetaData
                    .aggregateOperations(documentDbEnumerable.getList())
                    .collectionName(documentDbEnumerable.getCollectionName())
                    .metadataTable(documentDbEnumerable.getMetadataTable())
                    .build();
        }

        return null;
    }

    /**
     * Prepares the rootSchema so the PrepareContext uses the DocumentDB adapter.
     * Logic is lifted from Calcite's ModelHandler. It is simplified because we know that we
     * only care about the DocumentDb schema and do not have to visit any other potential schemas.
     */
    private static CalciteSchema getRootSchema(
            final DocumentDbDatabaseMetadata databaseMetadata,
            final DocumentDbConnectionProperties properties) {
        final SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
        final Schema schema = new DocumentDbSchemaFactory().create(databaseMetadata, properties);
        parentSchema.add(properties.getDatabase(), schema);
        return CalciteSchema.from(parentSchema);
    }

    /**
     * Our own implementation of CalcitePrepare.Context that doesn't need a Connection object.
     * Based on ContextImp in CalciteConnectionImpl.
     * PrepareContext is needed to leverage the CalcitePrepare API.
     */
    private static class DocumentDbPrepareContext implements CalcitePrepare.Context {
        private final CalciteSchema rootSchema;
        private final CalciteSchema mutableRootSchema;
        private final JavaTypeFactory typeFactory;
        private final CalciteConnectionConfig config;
        private final List<String> defaultSchemaPath;
        private final DataContext dataContext;

        DocumentDbPrepareContext(
                final CalciteSchema rootSchema,
                final String defaultSchema,
                final DocumentDbConnectionProperties properties) {
            this.typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            this.config = new CalciteConnectionConfigImpl(properties);
            final long now = System.currentTimeMillis();
            final SchemaVersion schemaVersion = new LongSchemaVersion(now);
            this.mutableRootSchema = rootSchema;
            this.rootSchema = mutableRootSchema.createSnapshot(schemaVersion);
            this.defaultSchemaPath = ImmutableList.of(defaultSchema);
            this.dataContext = new DataContext() {
                @Override
                public SchemaPlus getRootSchema() {
                    return rootSchema.plus();
                }

                @Override
                public JavaTypeFactory getTypeFactory() {
                    return typeFactory;
                }

                @Override
                public QueryProvider getQueryProvider() {
                    return null;
                }

                @Override
                public Object get(final String name) {
                    return null;
                }
            };
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }

        @Override
        public CalciteSchema getRootSchema() {
            return rootSchema;
        }

        @Override
        public CalciteSchema getMutableRootSchema() {
            return mutableRootSchema;
        }

        @Override
        public List<String> getDefaultSchemaPath() {
            return defaultSchemaPath;
        }

        @Override
        public CalciteConnectionConfig config() {
            return config;
        }

        @Override
        public SparkHandler spark() {
            final boolean enable = config().spark();
            return CalcitePrepare.Dummy.getSparkHandler(enable);
        }

        @Override
        public DataContext getDataContext() {
            return dataContext;
        }

        // This is also returned as null in ContextImp so this should be fine.
        @Override
        public List<String> getObjectPath() {
            return null;
        }

        // This seems to not be needed to get the functionality we want.
        @Override
        public RelRunner getRelRunner() {
            return null;
        }
    }
}
