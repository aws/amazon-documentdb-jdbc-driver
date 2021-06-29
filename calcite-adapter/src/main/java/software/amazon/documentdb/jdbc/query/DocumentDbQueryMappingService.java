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

package software.amazon.documentdb.jdbc.query;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalcitePrepare.CalciteSignature;
import org.apache.calcite.jdbc.CalcitePrepare.Query;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbEnumerable;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbSchemaFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbJdbcMetaDataConverter;

import java.sql.SQLException;
import java.util.List;


public class DocumentDbQueryMappingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbQueryMappingService.class);
    private final DocumentDbPrepareContext prepareContext;
    private final CalcitePrepare prepare;

    /**
     * Holds the DocumentDbDatabaseSchemaMetadata, CalcitePrepare.Context and the CalcitePrepare generated for a particular connection.
     * The default prepare factory is used like in CalciteConnectImpl.
     */
    public DocumentDbQueryMappingService(final DocumentDbConnectionProperties connectionProperties,
            final DocumentDbDatabaseSchemaMetadata databaseMetadata) {
        // Add MYSQL function support
        connectionProperties.putIfAbsent("FUN", "standard,mysql");
        this.prepareContext =
                new DocumentDbPrepareContext(
                        getRootSchemaFromDatabaseMetadata(connectionProperties, databaseMetadata),
                        connectionProperties.getDatabase(),
                        connectionProperties);
        this.prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    }

    /**
     * Uses CalcitePrepare API to parse and validate sql and convert to MQL.
     * @param sql the query in sql
     * @return the query context that has the target collection, aggregation stages, and result set metadata.
     */
    public DocumentDbMqlQueryContext get(final String sql) throws SQLException {
        final Query<Object> query = Query.of(sql);

        // In prepareSql:
        // -    We validate the sql based on the schema and turn this into a tree. (SQL->AST)
        // -    The query planner optimizes the tree with the DocumentDb adapter rules.
        // -    We visit each node and go into its implement method where the nodes become a physical
        // plan. (AST->MQL)
        try {
            final CalciteSignature<?> signature =
                    prepare.prepareSql(prepareContext, query, Object[].class, -1);

            // Enumerable contains the operations and fields we need to do the aggregation call.
            // Signature also contains a column list that has information about the columns/types of the
            // return row (ordinal, nullability, precision, etc).
            final Enumerable<?> enumerable = signature.enumerable(prepareContext.getDataContext());
            if (enumerable instanceof DocumentDbEnumerable) {
                final DocumentDbEnumerable documentDbEnumerable = (DocumentDbEnumerable) enumerable;
                return DocumentDbMqlQueryContext.builder()
                        .columnMetaData(DocumentDbJdbcMetaDataConverter.fromCalciteColumnMetaData(signature.columns))
                        .aggregateOperations(documentDbEnumerable.getList())
                        .collectionName(documentDbEnumerable.getCollectionName())
                        .paths(documentDbEnumerable.getPaths())
                        .build();
            }
        } catch (Exception e) {
            throw SqlError.createSQLException(
                    LOGGER, SqlState.INVALID_QUERY_EXPRESSION, e, SqlError.SQL_PARSE_ERROR, sql,
                    getExceptionMessages(e));
        }
        // Query could be parsed but cannot be executed in pure MQL (likely involves nested queries).
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_SQL, sql);
    }

    private String getExceptionMessages(final Throwable e) {
        final StringBuilder builder = new StringBuilder(e.getMessage());
        if (e.getSuppressed() != null) {
            for (Throwable suppressed : e.getSuppressed()) {
                builder.append(" Additional info: '")
                .append(getExceptionMessages(suppressed))
                .append("'");
            }
        }
        return builder.toString();
    }

    /**
     * Creates a {@link CalciteSchema} from the database metadata.
     * @param databaseMetadata the metadata for the target database.
     * @return a {@link CalciteSchema} for the database described by the databaseMetadata.
     */
    private static CalciteSchema getRootSchemaFromDatabaseMetadata(
            final DocumentDbConnectionProperties connectionProperties,
            final DocumentDbDatabaseSchemaMetadata databaseMetadata) {
        final SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
        final Schema schema = new DocumentDbSchemaFactory().create(databaseMetadata, connectionProperties);
        parentSchema.add(connectionProperties.getDatabase(), schema);
        return CalciteSchema.from(parentSchema);
    }

    /**
     * Our own implementation of {@link CalcitePrepare.Context} to pass the schema without a {@link java.sql.Connection}.
     * Based on the prepare context in CalciteConnectionImpl.
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
