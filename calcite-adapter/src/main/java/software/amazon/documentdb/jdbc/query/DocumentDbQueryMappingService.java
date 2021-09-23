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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
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
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.impl.LongSchemaVersion;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelRunner;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbEnumerable;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbSchemaFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbJdbcMetaDataConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentDbQueryMappingService implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbQueryMappingService.class);
    private final DocumentDbPrepareContext prepareContext;
    private final CalcitePrepare prepare;
    private final MongoClient client;
    private final boolean closeClient;
    private BsonDocument maxRowsBSON;

    /**
     * Holds the DocumentDbDatabaseSchemaMetadata, CalcitePrepare.Context and the CalcitePrepare
     * generated for a particular connection.
     * The default prepare factory is used like in CalciteConnectImpl.
     *
     * @param connectionProperties the connection properties.
     * @param databaseMetadata the database schema metadata.
     * @param client the {@link MongoClient} client.
     */
    public DocumentDbQueryMappingService(final DocumentDbConnectionProperties connectionProperties,
            final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final MongoClient client) {
        // Add MYSQL function support
        connectionProperties.putIfAbsent("FUN", "standard,mysql");
        // Leave unquoted identifiers in their original case. Identifiers are still case-sensitive
        // but do not need to be quoted
        connectionProperties.putIfAbsent("UNQUOTEDCASING", "UNCHANGED");
        // Initialize the MongoClient
        this.client = client != null
                ? client
                : MongoClients.create(connectionProperties.buildMongoClientSettings());
        this.closeClient = client == null;
        this.prepareContext =
                new DocumentDbPrepareContext(
                        getRootSchemaFromDatabaseMetadata(connectionProperties, databaseMetadata, this.client),
                        connectionProperties.getDatabase(),
                        connectionProperties);
        this.prepare = new DocumentDbPrepareImplementation();
        this.maxRowsBSON = new BsonDocument();
    }

    /**
     * Uses CalcitePrepare API to parse and validate sql and convert to MQL.
     * @param sql the query in sql
     * @param maxRowCount the max number of rows to return
     * @return the query context that has the target collection, aggregation stages, and result set metadata.
     */
    public DocumentDbMqlQueryContext get(final String sql, final long maxRowCount) throws SQLException {
        final Query<Object> query = Query.of(sql);

        // In prepareSql:
        // -    We validate the sql based on the schema and turn this into a tree. (SQL->AST)
        // -    The query planner optimizes the tree with the DocumentDb adapter rules.
        // -    We visit each node and go into its implement method where the nodes become a physical
        // plan. (AST->MQL)
        try {
            // The parameter maxRowCount from prepareSql needs to be -1, we are handling max rows
            // outside calcite translation
            final CalciteSignature<?> signature =
                    prepare.prepareSql(prepareContext, query, Object[].class, -1);

            // Enumerable contains the operations and fields we need to do the aggregation call.
            // Signature also contains a column list that has information about the columns/types of the
            // return row (ordinal, nullability, precision, etc).
            final Enumerable<?> enumerable = signature.enumerable(prepareContext.getDataContext());
            if (enumerable instanceof DocumentDbEnumerable) {
                final DocumentDbEnumerable documentDbEnumerable = (DocumentDbEnumerable) enumerable;

                // Add limit if using setMaxRows.
                if (maxRowCount > 0) {
                    maxRowsBSON.put("$limit",new BsonInt64(maxRowCount));
                    documentDbEnumerable.getList().add(maxRowsBSON);
                }

                return DocumentDbMqlQueryContext.builder()
                        .columnMetaData(DocumentDbJdbcMetaDataConverter.fromCalciteColumnMetaData(signature.columns))
                        .aggregateOperations(documentDbEnumerable.getList())
                        .collectionName(documentDbEnumerable.getCollectionName())
                        .paths(documentDbEnumerable.getPaths())
                        .build();
            }
        } catch (Exception e) {
            // TODO: AD-273 Fix this error handling.
            throw SqlError.createSQLException(
                    LOGGER, SqlState.INVALID_QUERY_EXPRESSION, e, SqlError.SQL_PARSE_ERROR, sql,
                    getExceptionMessages(e));
        }
        // Query could be parsed but cannot be executed in pure MQL (likely involves nested queries).
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.UNSUPPORTED_SQL, sql);
    }

    /**
     * Uses CalcitePrepare API to parse and validate sql and convert to MQL.
     * Assumes no max row count set.
     * @param sql the query in sql
     * @return the query context that has the target collection, aggregation stages, and result set metadata.
     */
    public DocumentDbMqlQueryContext get(final String sql) throws SQLException {
        return get(sql, 0);
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
            final DocumentDbDatabaseSchemaMetadata databaseMetadata,
            final MongoClient client) {
        final SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
        final Schema schema = DocumentDbSchemaFactory
                .create(databaseMetadata, connectionProperties, client);
        parentSchema.add(connectionProperties.getDatabase(), schema);
        return CalciteSchema.from(parentSchema);
    }

    @Override
    public void close() {
        if (closeClient && client != null) {
            client.close();
        }
    }

    /**
     * Our own implementation of {@link RelDataTypeSystem}.
     * All settings are the same as the default unless otherwise overridden.
     */
    private static class DocumentDbTypeSystem extends RelDataTypeSystemImpl implements RelDataTypeSystem {

        /**
         * Returns whether the least restrictive type of a number of CHAR types of different lengths
         * should be a VARCHAR type.
         * @return true to be consistent with SQLServer, MySQL and other major DBMS.
         */
        @Override
        public boolean shouldConvertRaggedUnionTypesToVarying() {
            return true;
        }
    }

    /**
     * Our own implementation of {@link CalcitePrepare}.
     * Extends {@link org.apache.calcite.prepare.CalcitePrepareImpl}.
     * All settings are the same as the default unless otherwise overridden.
     */
    private static class DocumentDbPrepareImplementation extends CalcitePrepareImpl implements CalcitePrepare {

        @Override
        protected SqlRexConvertletTable createConvertletTable() {
            return DocumentDbConvertletTable.INSTANCE;
        }
    }

    /**
     * Our own implementation of {@link SqlRexConvertletTable}.
     * Behaviour is the same as {@link StandardConvertletTable} unless operator is part of custom map.
     */
    private static final class DocumentDbConvertletTable implements SqlRexConvertletTable {

        public static final DocumentDbConvertletTable INSTANCE = new DocumentDbConvertletTable();
        private final Map<SqlOperator, SqlRexConvertlet> customCovertlets = new HashMap<>();

        private DocumentDbConvertletTable() {
            customCovertlets.put(SqlStdOperatorTable.TIMESTAMP_DIFF, new DocumentDbTimestampDiffConvertlet());
        }

        @Override
        public SqlRexConvertlet get(final SqlCall call) {
            // Check if we override the operator conversion. Otherwise use standard conversion.
            final SqlOperator op = call.getOperator();
            final SqlRexConvertlet convertlet = customCovertlets.get(op);
            if (convertlet != null) {
                return convertlet;
            }

            return StandardConvertletTable.INSTANCE.get(call);
        }

        /**
         * Replaces the TimestampDiffConvertlet in {@link StandardConvertletTable}.
         * Overrides the translation of TIMESTAMPDIFF for YEAR, QUARTER, and MONTH.
         * Implementation copied from original but adds lines 259-261.
         */
        private static class DocumentDbTimestampDiffConvertlet implements SqlRexConvertlet {
            public RexNode convertCall(final SqlRexContext cx, final SqlCall call) {
                // TIMESTAMPDIFF(unit, t1, t2) => (t2 - t1) UNIT
                final RexBuilder rexBuilder = cx.getRexBuilder();
                final SqlLiteral unitLiteral = call.operand(0);
                TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
                final SqlTypeName sqlTypeName = unit == TimeUnit.NANOSECOND
                        ? SqlTypeName.BIGINT
                        : SqlTypeName.INTEGER;
                final BigDecimal multiplier;
                final BigDecimal divider;
                switch (unit) {
                    case MICROSECOND:
                    case MILLISECOND:
                    case NANOSECOND:
                    case WEEK:
                        multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
                        divider = unit.multiplier;
                        unit = TimeUnit.SECOND;
                        break;
                    default:
                        multiplier = BigDecimal.ONE;
                        divider = BigDecimal.ONE;
                }
                final SqlIntervalQualifier qualifier =
                        new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
                final RexNode op2 = cx.convertExpression(call.operand(2));
                final RexNode op1 = cx.convertExpression(call.operand(1));
                final RelDataType intervalType =
                        cx.getTypeFactory().createTypeWithNullability(
                                cx.getTypeFactory().createSqlIntervalType(qualifier),
                                op1.getType().isNullable() || op2.getType().isNullable());
                final RexCall rexCall = (RexCall) rexBuilder.makeCall(
                        intervalType, SqlStdOperatorTable.MINUS_DATE,
                        ImmutableList.of(op2, op1));
                final RelDataType intType =
                        cx.getTypeFactory().createTypeWithNullability(
                                cx.getTypeFactory().createSqlType(sqlTypeName),
                                SqlTypeUtil.containsNullable(rexCall.getType()));

                // If dealing with year, quarter, or month we will calculate the difference using date parts
                // and do not need any integer division.
                if (unit == TimeUnit.YEAR || unit == TimeUnit.QUARTER || unit == TimeUnit.MONTH) {
                    return rexBuilder.makeReinterpretCast(intType, rexCall, rexBuilder.makeLiteral(false));
                }

                final RexNode e = rexBuilder.makeCast(intType, rexCall);
                return rexBuilder.multiplyDivide(e, multiplier, divider);
            }
        }
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
            this.typeFactory = new JavaTypeFactoryImpl(new DocumentDbTypeSystem());
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
