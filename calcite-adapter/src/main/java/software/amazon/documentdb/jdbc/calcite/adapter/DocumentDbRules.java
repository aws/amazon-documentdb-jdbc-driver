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

import com.google.common.io.BaseEncoding;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable.NullAs;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.bson.BsonType;
import org.slf4j.Logger;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.sql.SQLFeatureNotSupportedException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.Month;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * Rules and relational operators for
 * {@link DocumentDbRel#CONVENTION MONGO}
 * calling convention.
 */
public final class DocumentDbRules {
    private static final Logger LOGGER = CalciteTrace.getPlannerTracer();
    private static final Pattern OBJECT_ID_PATTERN = Pattern.compile("^[0-9a-zA-Z]{24}$");

    private DocumentDbRules() { }

    @SuppressWarnings("MutablePublicArray")
    static final RelOptRule[] RULES = {
            DocumentDbSortRule.INSTANCE,
            DocumentDbFilterRule.INSTANCE,
            DocumentDbProjectRule.INSTANCE,
            DocumentDbAggregateRule.INSTANCE,
            DocumentDbJoinRule.INSTANCE
    };

    // Factors for computing the cost of the DocumentDbRel nodes.
    public static final double PROJECT_COST_FACTOR = 0.1;
    public static final double FILTER_COST_FACTOR = 0.1;
    public static final double JOIN_COST_FACTOR = 0.1;
    public static final double SORT_COST_FACTOR = 0.05;
    public static final double ENUMERABLE_COST_FACTOR = 0.1;

    public static final int MAX_PROJECT_FIELDS = 50;

    /** Returns 'string' if it is a call to item['string'], null otherwise. */
    static String isItem(final RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.ITEM) {
            return null;
        }
        final RexNode op0 = call.operands.get(0);
        final RexNode op1 = call.operands.get(1);
        if (op0 instanceof RexInputRef
                && ((RexInputRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue2() instanceof String) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        return null;
    }

    // DocumentDB: modified - start
    static List<String> mongoFieldNames(final RelDataType rowType,
            final DocumentDbSchemaTable metadataTable, final boolean useOriginalPaths) {
        // DocumentDB: modified - end
        return new AbstractList<String>() {
            @Override public String get(final int index) {
                // DocumentDB: modified - start
                final String name = rowType.getFieldList().get(index).getName();
                final DocumentDbSchemaColumn column = metadataTable.getColumnMap().get(name);

                // Null columns are assumed to be fields generated by the query
                // such as aggregate expressions (ex: COUNT(*)).
                if (column == null) {
                    return getNormalizedIdentifier(name);
                }

                return getPath(column, useOriginalPaths);
                // DocumentDB: modified - end
            }

            @Override public int size() {
                return rowType.getFieldCount();
            }
        };
    }

    static String getPath(final DocumentDbSchemaColumn column, final boolean useOriginalPaths) {
        final String path;
        if (column instanceof DocumentDbMetadataColumn
                && (!isNullOrWhitespace(((DocumentDbMetadataColumn) column).getResolvedPath())
                && !useOriginalPaths)) {
            path = ((DocumentDbMetadataColumn) column).getResolvedPath();
        } else if (column.isIndex()) {
            path = column.getSqlName();
        } else {
            path = column.getFieldPath();
        }
        if (isNullOrWhitespace(path)) {
            return null;
        }
        return path;
    }
    static List<String> mongoFieldNames(final RelDataType rowType, final DocumentDbSchemaTable metadataTable) {
        return mongoFieldNames(rowType, metadataTable, false);
    }

    static String maybeQuote(final String s) {
        if (!needsQuote(s)) {
            return s;
        }
        return quote(s);
    }

    static String quote(final String s) {
        return "'" + s + "'"; // TODO: handle embedded quotes
    }

    private static boolean needsQuote(final String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            final char c = s.charAt(i);
            // DocumentDB: modified - start
            // Add quotes for embedded documents (contains '.') and
            // for field names with ':'.
            if (!Character.isJavaIdentifierPart(c)
                    || c == '$' || c == '.' || c == ':') {
                return true;
            }
            // DocumentDB: modified - end
        }
        return false;
    }

    /**
     * Removes the '$' symbol from the start of a string, and replaces it with '_'.
     * @param fieldName The non-normalized string
     * @return The input string with '$' replaced by '_'
     */
    protected static String getNormalizedIdentifier(final String fieldName) {
        return fieldName.startsWith("$") ? "_" + fieldName.substring(1) : fieldName;
    }

    /** Translator from {@link RexNode} to strings in MongoDB's expression
     * language. */
    static class RexToMongoTranslator extends RexVisitorImpl<Operand> {
        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;
        private final List<String> keys;
        private final DocumentDbSchemaTable schemaTable;
        private final Map<SqlOperator,
                BiFunction<RexCall, List<Operand>, Operand>> rexCallToMongoMap = new HashMap<>();

        private static final Map<SqlOperator, String> MONGO_OPERATORS =
                new HashMap<>();

        static {
            // Arithmetic
            MONGO_OPERATORS.put(SqlStdOperatorTable.DIVIDE, "$divide");
            MONGO_OPERATORS.put(SqlStdOperatorTable.MULTIPLY, "$multiply");
            MONGO_OPERATORS.put(SqlStdOperatorTable.MOD, "$mod");
            MONGO_OPERATORS.put(SqlStdOperatorTable.PLUS, "$add");
            MONGO_OPERATORS.put(SqlStdOperatorTable.MINUS, "$subtract");
            MONGO_OPERATORS.put(SqlStdOperatorTable.MINUS_DATE, "$subtract");
            // Boolean
            MONGO_OPERATORS.put(SqlStdOperatorTable.AND, "$and");
            MONGO_OPERATORS.put(SqlStdOperatorTable.OR, "$or");
            MONGO_OPERATORS.put(SqlStdOperatorTable.NOT, "$not");
            // Comparison
            MONGO_OPERATORS.put(SqlStdOperatorTable.EQUALS, "$eq");
            MONGO_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, "$ne");
            MONGO_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, "$gt");
            MONGO_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$gte");
            MONGO_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, "$lt");
            MONGO_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$lte");

            MONGO_OPERATORS.put(SqlStdOperatorTable.IS_NULL, "$lte");
            MONGO_OPERATORS.put(SqlStdOperatorTable.IS_NOT_NULL, "$gt");

        }

        private void initializeRexCallToMongoMap(final Instant currentTime) {
            // Arithmetic
            rexCallToMongoMap.put(SqlStdOperatorTable.DIVIDE,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.MULTIPLY,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.MOD,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.PLUS,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.MINUS,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.DIVIDE_INTEGER,
                    RexToMongoTranslator::getMongoAggregateForIntegerDivide);
            // Boolean
            rexCallToMongoMap.put(SqlStdOperatorTable.AND,
                    (call, strings) -> getMongoAggregateForAndOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.OR,
                    (call, strings) -> getMongoAggregateForOrOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.NOT,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            // Comparison
            rexCallToMongoMap.put(SqlStdOperatorTable.EQUALS,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            // Need to handle null value
            rexCallToMongoMap.put(SqlStdOperatorTable.NOT_EQUALS,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.GREATER_THAN,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.LESS_THAN,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));

            rexCallToMongoMap.put(SqlStdOperatorTable.IS_NULL,
                    (call, strings) -> getMongoAggregateForNullOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            rexCallToMongoMap.put(SqlStdOperatorTable.IS_NOT_NULL,
                    (call, strings) -> getMongoAggregateForNullOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));

            // Date operations
            rexCallToMongoMap.put(SqlStdOperatorTable.CURRENT_DATE,
                    (call, operands) -> DateFunctionTranslator.translateCurrentTimestamp(currentTime));
            rexCallToMongoMap.put(SqlStdOperatorTable.CURRENT_TIME,
                    (call, operands) -> DateFunctionTranslator.translateCurrentTimestamp(currentTime));
            rexCallToMongoMap.put(SqlStdOperatorTable.CURRENT_TIMESTAMP,
                    (call, operands) -> DateFunctionTranslator.translateCurrentTimestamp(currentTime));
            rexCallToMongoMap.put(SqlStdOperatorTable.DATETIME_PLUS, DateFunctionTranslator::translateDateAdd);
            rexCallToMongoMap.put(SqlStdOperatorTable.EXTRACT, DateFunctionTranslator::translateExtract);
            rexCallToMongoMap.put(SqlLibraryOperators.DAYNAME, DateFunctionTranslator::translateDayName);
            rexCallToMongoMap.put(SqlLibraryOperators.MONTHNAME, DateFunctionTranslator::translateMonthName);
            rexCallToMongoMap.put(SqlStdOperatorTable.FLOOR, DateFunctionTranslator::translateFloor);
            rexCallToMongoMap.put(SqlStdOperatorTable.MINUS_DATE, DateFunctionTranslator::translateDateDiff);

            // CASE, ITEM
            rexCallToMongoMap.put(SqlStdOperatorTable.CASE, RexToMongoTranslator::getMongoAggregateForCase);
            rexCallToMongoMap.put(SqlStdOperatorTable.ITEM, RexToMongoTranslator::getMongoAggregateForItem);

            rexCallToMongoMap.put(SqlStdOperatorTable.SUBSTRING,
                    RexToMongoTranslator::getMongoAggregateForSubstringOperator);
        }

        private static Operand getMongoAggregateForAndOperator(final RexCall call, final List<Operand> operands, final String s) {
            final StringBuilder sb = new StringBuilder();
            sb.append("{$cond: [{$and: [");
            for (Operand value: operands) {
                sb.append("{$eq: [true, ").append(value).append("]},");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]}, true,");
            sb.append("{$cond: [{$or: [");
            for (Operand value: operands) {
                sb.append("{$eq: [false, ").append(value).append("]},");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]}, false, null]}]}");

            return new Operand(
                    sb.toString(),
                    SimpleMatchTranslator.getAndOrOperator(operands, s),
                    false);
        }

        private static Operand getMongoAggregateForOrOperator(final RexCall call, final List<Operand> operands, final String s) {
            final StringBuilder sb = new StringBuilder();
            sb.append("{$cond: [{$or: [");
            for (Operand value: operands) {
                sb.append("{$eq: [true, ").append(value.getAggregationValue()).append("]},");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]}, true,");
            sb.append("{$cond: [{$and: [");
            for (Operand value: operands) {
                sb.append("{$eq: [false, ").append(value.getAggregationValue()).append("]},");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]}, false, null]}]}");
            return new Operand(
                    sb.toString(),
                    SimpleMatchTranslator.getAndOrOperator(operands, s),
                    false);
        }

        protected RexToMongoTranslator(final JavaTypeFactory typeFactory,
                final List<String> inFields,
                final List<String> keys, final DocumentDbSchemaTable schemaTable,
                final Instant currentTime) {
            super(true);
            initializeRexCallToMongoMap(currentTime);
            this.typeFactory = typeFactory;
            this.inFields = inFields;
            this.keys = keys;
            this.schemaTable = schemaTable;
        }

        @Override public Operand visitLiteral(final RexLiteral literal) {
            if (literal.getValue() == null) {
                return new Operand("null");
            }

            switch (literal.getType().getSqlTypeName()) {
                case DECIMAL:
                case DOUBLE:
                case FLOAT:
                case REAL:
                    final String doubleFormat = "{\"$numberDouble\": \"" + literal.getValueAs(Double.class) + "\"}";
                    return new Operand(doubleFormat, doubleFormat, true);
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    // Convert supported intervals to milliseconds.
                    final String intFormat = "{\"$numberInt\": \"" + literal.getValueAs(Long.class) + "\"}";
                    return new Operand("{\"$literal\": " + intFormat + "}", intFormat, true);
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_HOUR:
                case INTERVAL_MINUTE:
                case INTERVAL_SECOND:
                    // Convert supported intervals to milliseconds.
                    final String longFormat = "{\"$numberLong\": \"" + literal.getValueAs(Long.class) + "\"}";
                    return new Operand("{\"$literal\": " + longFormat + "}", longFormat, true);
                case DATE:
                    // NOTE: Need to get the number of milliseconds from Epoch (not # of days).
                    final String dateFormat = "{\"$date\": {\"$numberLong\": \"" + literal.getValueAs(DateString.class).getMillisSinceEpoch() + "\" } }";
                    return new Operand(dateFormat, dateFormat, true);
                case TIME:
                    // NOTE: Need to get the number of milliseconds from day. Date portion is left as zero epoch.
                    final String timeFormat = "{\"$date\": {\"$numberLong\": \"" + literal.getValueAs(TimeString.class).getMillisOfDay() + "\" } }";
                    return new Operand(timeFormat, timeFormat, true);
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // Convert from date in milliseconds to MongoDb date.
                    final String datetimeFormat = "{\"$date\": {\"$numberLong\": \"" + literal.getValueAs(Long.class) + "\" } }";
                    return new Operand(datetimeFormat, datetimeFormat, true);
                case BINARY:
                case VARBINARY:
                    final String base64Literal = BaseEncoding.base64()
                            .encode(literal.getValueAs(byte[].class));
                    final String binaryFormat = "{\"$binary\": {\"base64\": \""
                            + base64Literal
                            + "\", \"subType\": \"00\"}}";
                    return new Operand(binaryFormat, binaryFormat, true);
                default:
                    final String simpleLiteral = RexToLixTranslator.translateLiteral(literal, literal.getType(),
                            typeFactory, NullAs.NOT_POSSIBLE).toString();
                    return new Operand("{\"$literal\": " + simpleLiteral + "}", simpleLiteral, true);
            }
        }

        @Override public Operand visitInputRef(final RexInputRef inputRef) {
            // NOTE: Pass the column metadata with the operand.
            return new Operand(
                    maybeQuote("$" + inFields.get(inputRef.getIndex())),
                    maybeQuote(inFields.get(inputRef.getIndex())),
                    false,
                    schemaTable.getColumnMap().get(keys.get(inputRef.getIndex())));
        }

        @SneakyThrows
        @Override public Operand visitCall(final RexCall call) {
            final String name = isItem(call);
            if (name != null) {
                return new Operand("'$" + name + "'");
            }

            final List<Operand> strings = visitList(call.operands);
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.REINTERPRET) {
                // TODO: Handle case when DocumentDB supports $convert.
                return strings.get(0);
            }

            if (rexCallToMongoMap.containsKey(call.getOperator())) {
                final Operand result = rexCallToMongoMap.get(call.getOperator()).apply(call, strings);
                if (result != null) {
                    return result;
                }
            }

            throw new IllegalArgumentException("Translation of " + call
                    + " is not supported by DocumentDbRules");
        }

        private static Operand getMongoAggregateForIntegerDivide(final RexCall call, final List<Operand> strings) {
            return getIntegerDivisionOperation(strings.get(0), strings.get(1));
        }

        private static Operand getMongoAggregateForCase(
                final RexCall call,
                final List<Operand> strings) {
            final StringBuilder sb = new StringBuilder();
            final StringBuilder finish = new StringBuilder();
            // case(a, b, c)  -> $cond:[a, b, c]
            // case(a, b, c, d) -> $cond:[a, b, $cond:[c, d, null]]
            // case(a, b, c, d, e) -> $cond:[a, b, $cond:[c, d, e]]
            for (int i = 0; i < strings.size(); i += 2) {
                sb.append("{$cond:[");
                finish.append("]}");

                sb.append(strings.get(i));
                sb.append(',');
                sb.append(strings.get(i + 1));
                sb.append(',');
                if (i == strings.size() - 3) {
                    sb.append(strings.get(i + 2));
                    break;
                }
                if (i == strings.size() - 2) {
                    sb.append("null");
                    break;
                }
            }
            sb.append(finish);
            return new Operand(sb.toString());
        }

        private static Operand getMongoAggregateForItem(
                final RexCall call,
                final List<Operand> strings) {
            final RexNode op1 = call.operands.get(1);
            if (op1 instanceof RexLiteral
                    && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                if (!Bug.CALCITE_194_FIXED) {
                    return new Operand("'" + stripQuotes(strings.get(0).getAggregationValue()) + "["
                            + ((RexLiteral) op1).getValue2() + "]'");
                }
                return new Operand(strings.get(0) + "[" + strings.get(1) + "]");
            }
            return null;
        }

        @SneakyThrows
        private static Operand getMongoAggregateForComparisonOperator(
                final RexCall call,
                final List<Operand> strings,
                final String stdOperator) {
            // {$cond: [<null check expression>, <comparison expression>, null]}
            final String aggregateExpr = "{\"$cond\": ["
                                    + getNullCheckExpr(strings)
                                    + ", "
                                    + getMongoAggregateForOperator(call, strings, stdOperator)
                                    + ", null]}";

            return new Operand(
                    aggregateExpr,
                    hasObjectIdAndLiteral(call, strings)
                            ? SimpleMatchTranslator.getObjectIdComparisonOperator(call, strings, stdOperator)
                            : SimpleMatchTranslator.getComparisonOperator(call, strings, stdOperator),
                    false);
        }

        private static String getNullCheckExpr(final List<Operand> strings) {
            final StringBuilder nullCheckOperator = new StringBuilder("{\"$and\": [");
            for (Operand s : strings) {
                nullCheckOperator.append("{\"$gt\": [");
                nullCheckOperator.append(s);
                nullCheckOperator.append(", null]},");
            }
            nullCheckOperator.deleteCharAt(nullCheckOperator.length() - 1);
            nullCheckOperator.append("]}");
            return nullCheckOperator.toString();
        }

        private static Operand getMongoAggregateForNullOperator(
                final RexCall call,
                final List<Operand> strings,
                final String stdOperator) {
            return new Operand(
                    "{" + stdOperator + ": [" + strings.get(0) + ", null]}",
                    SimpleMatchTranslator.getNullCheckOperator(call, strings), false);
        }

        private static Operand getMongoAggregateForSubstringOperator(
                final RexCall call,
                final List<Operand> strings) {
            final List<Operand> inputs = new ArrayList<>(strings);
            inputs.set(1, new Operand("{$subtract: [" + inputs.get(1) + ", 1]}")); // Conversion from one-indexed to zero-indexed
            if (inputs.size() == 2) {
                inputs.add(new Operand(String.valueOf(Integer.MAX_VALUE)));
            }
            return new Operand("{$substrCP: [" + Util.commaList(inputs) + "]}");
        }

    }

    private static String stripQuotes(final String s) {
        return s.startsWith("'") && s.endsWith("'")
                ? s.substring(1, s.length() - 1)
                : s;
    }

    @SneakyThrows
    private static Operand getMongoAggregateForOperator(
            final RexCall call,
            final List<Operand> strings,
            final String stdOperator) {
        verifySupportedType(call);
        if (hasObjectIdAndLiteral(call, strings)) {
            return new Operand(getObjectIdAggregateForOperator(call, strings, stdOperator));
        }
        return new Operand("{" + maybeQuote(stdOperator) + ": [" + Util.commaList(strings) + "]}");
    }

    private static void verifySupportedType(final RexCall call)
            throws SQLFeatureNotSupportedException {
        if (call.type.getSqlTypeName() == SqlTypeName.INTERVAL_MONTH
                || call.type.getSqlTypeName() == SqlTypeName.INTERVAL_YEAR) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER,
                    SqlError.UNSUPPORTED_CONVERSION,
                    call.type.getSqlTypeName().getName(),
                    SqlTypeName.TIMESTAMP.getName());
        }
    }

    private static Operand getIntegerDivisionOperation(final String value, final String divisor) {
        // TODO: when $trunc is supported in DocumentDB, add back.
        //final String intDivideOptFormat = "{ \"$trunc\": [ {\"$divide\": [%s]}, 0 ]}";
        // NOTE: $mod, $subtract, and $divide - together, perform integer division
        final String modulo = String.format(
                "{\"$mod\": [%s, %s]}", value, divisor);
        final String subtractRemainder = String.format(
                "{\"$subtract\": [%s, %s]}", value, modulo);
        return Operand.format("{\"$divide\": [%s, %s]}", subtractRemainder, divisor);
    }

    private static Operand getIntegerDivisionOperation(final Operand value, final Operand divisor) {
        return getIntegerDivisionOperation(value.getAggregationValue(), divisor.getAggregationValue());
    }

    private static String getObjectIdAggregateForOperator(
            final RexCall call,
            final List<Operand> strings,
            final String stdOperator) {
        // $or together the $oid and native operations.
        final String oidOperation = "{" + maybeQuote(stdOperator)
                + ": [" + Util.commaList(reformatObjectIdOperands(call, strings)) + "]}";
        final String nativeOperation = "{" + maybeQuote(stdOperator)
                + ": [" + Util.commaList(strings) + "]}";
        return "{\"$or\": [" + oidOperation + ", " + nativeOperation + "]}";
    }

    private static boolean hasObjectIdAndLiteral(
            final RexCall call,
            final List<Operand> strings) {
        final Operand objectIdOperand = strings.stream()
                .filter(operand -> operand.getColumn() != null
                        && operand.getColumn().getDbType() == BsonType.OBJECT_ID)
                .findFirst().orElse(null);
        if (objectIdOperand == null) {
            return false;
        }
        for (int index = 0; index < strings.size(); index++) {
            final Operand operand = strings.get(index);
            if (operand == objectIdOperand || !(call.operands.get(index) instanceof RexLiteral)) {
                continue;
            }
            final RexLiteral literal = (RexLiteral) call.operands.get(index);
            switch (literal.getTypeName()) {
                case BINARY:
                case VARBINARY:
                    final byte[] valueAsByteArray = literal.getValueAs(byte[].class);
                    if (valueAsByteArray.length == 12) {
                        return true;
                    }
                    break;
                case CHAR:
                case VARCHAR:
                    final String valueAsString = literal.getValueAs(String.class);
                    if (OBJECT_ID_PATTERN.matcher(valueAsString).matches()) {
                        return true;
                    }
                    break;
            }
        }
        return false;
    }

    private static List<Operand> reformatObjectIdOperands(
            final RexCall call,
            final List<Operand> strings) {
        final List<Operand> copyOfStrings = new ArrayList<>();
        for (int index = 0; index < strings.size(); index++) {
            final Operand operand = strings.get(index);
            if (call.operands.get(index) instanceof RexLiteral) {
                final RexLiteral literal = (RexLiteral) call.operands.get(index);
                copyOfStrings.add(reformatObjectIdLiteral(literal, operand));
            } else {
                copyOfStrings.add(operand);
            }
        }
        return copyOfStrings;
    }

    private static Operand reformatObjectIdLiteral(
            final RexLiteral literal,
            final Operand operand) {
        switch (literal.getTypeName()) {
            case BINARY:
            case VARBINARY:
                final byte[] valueAsByteArray = literal.getValueAs(byte[].class);
                if (valueAsByteArray.length == 12) {
                    final String value = "{\"$oid\": \""
                            + BaseEncoding.base16().encode(valueAsByteArray)
                            + "\"}";
                    return new Operand(value, value, true);
                } else {
                    return operand;
                }
            case CHAR:
            case VARCHAR:
                final String valueAsString = literal.getValueAs(String.class);
                if (OBJECT_ID_PATTERN.matcher(valueAsString).matches()) {
                    final String value = "{\"$oid\": \"" + valueAsString + "\"}";
                    return new Operand(value, value, true);
                } else {
                    return operand;
                }
            default:
                return operand;
        }
    }

    private static class DateFunctionTranslator {

        private static final Map<TimeUnitRange, String> DATE_PART_OPERATORS =
                new HashMap<>();
        private static final Instant FIRST_DAY_OF_WEEK_AFTER_EPOCH =
                Instant.parse("1970-01-05T00:00:00Z");

        static {
            // Date part operators
            DATE_PART_OPERATORS.put(TimeUnitRange.YEAR, "$year");
            DATE_PART_OPERATORS.put(TimeUnitRange.MONTH, "$month");
            DATE_PART_OPERATORS.put(TimeUnitRange.WEEK, "$week");
            DATE_PART_OPERATORS.put(TimeUnitRange.HOUR, "$hour");
            DATE_PART_OPERATORS.put(TimeUnitRange.MINUTE, "$minute");
            DATE_PART_OPERATORS.put(TimeUnitRange.SECOND, "$second");
            DATE_PART_OPERATORS.put(TimeUnitRange.DOY, "$dayOfYear");
            DATE_PART_OPERATORS.put(TimeUnitRange.DAY, "$dayOfMonth");
            DATE_PART_OPERATORS.put(TimeUnitRange.DOW, "$dayOfWeek");
            DATE_PART_OPERATORS.put(TimeUnitRange.ISODOW, "$isoDayOfWeek");
            DATE_PART_OPERATORS.put(TimeUnitRange.ISOYEAR, "$isoWeekYear");
        }

        private static Operand translateCurrentTimestamp(final Instant currentTime) {
            final String currentTimestamp = "{\"$date\": {\"$numberLong\": "
                    + "\"" + currentTime.toEpochMilli() + "\"}}";
            return new Operand(currentTimestamp, currentTimestamp, true);
        }

        private static Operand translateDateAdd(final RexCall call, final List<Operand> strings) {
            // TODO: Check for unsupported intervals and throw error/emulate in some other way.
            return new Operand("{ \"$add\":" + "[" + Util.commaList(strings) + "]}");
        }

        private static Operand translateDateDiff(final RexCall call, final List<Operand> strings) {
            final TimeUnitRange interval = call.getType().getIntervalQualifier().timeUnitRange;
            switch (interval) {
                case YEAR:
                    return formatDateDiffYear(strings);
                case QUARTER:
                case MONTH:
                    return formatDateDiffMonth(strings, interval);
                default:
                    return getMongoAggregateForOperator(
                            call,
                            strings,
                            RexToMongoTranslator.MONGO_OPERATORS.get(SqlStdOperatorTable.MINUS_DATE));
            }
        }

        private static Operand formatDateDiffYear(final List<Operand> strings) {
            final String dateDiffYearFormat =
                    "{'$subtract': [{'$year': %1$s}, {'$year': %2$s}]}";
            return Operand.format(dateDiffYearFormat, strings.get(0), strings.get(1));
        }

        private static Operand formatDateDiffMonth(final List<Operand> strings, final TimeUnitRange timeUnitRange) {
            final String yearPartMultiplier = timeUnitRange == TimeUnitRange.QUARTER ? "4" : "12";
            final String monthPart1 =
                    timeUnitRange == TimeUnitRange.QUARTER
                            ? translateExtractQuarter(strings.get(0)).getAggregationValue()
                            : String.format("{'$month': %s}", strings.get(0));
            final String monthPart2 =
                    timeUnitRange == TimeUnitRange.QUARTER
                            ? translateExtractQuarter(strings.get(1)).getAggregationValue()
                            : String.format("{'$month': %s}", strings.get(1));
            final String dateDiffMonthFormat =
                    "{'$subtract': [ "
                            + "{'$add': [ "
                            + "{'$multiply': [%1$s, {'$year': %2$s}]}, "
                            + "%4$s]}, "
                            + "{'$add': [ "
                            + "{'$multiply': [%1$s, {'$year': %3$s}]}, "
                            + "%5$s]}]}";
            return Operand.format(
                    dateDiffMonthFormat,
                    yearPartMultiplier,
                    strings.get(0),
                    strings.get(1),
                    monthPart1,
                    monthPart2);
        }

        private static Operand translateExtract(final RexCall call, final List<Operand> strings) {
            // The first argument to extract is the interval (literal)
            // and the second argument is the date (can be any node evaluating to a date).
            final RexLiteral literal = (RexLiteral) call.getOperands().get(0);
            final TimeUnitRange range = literal.getValueAs(TimeUnitRange.class);

            if (range == TimeUnitRange.QUARTER) {
                return translateExtractQuarter(strings.get(1));
            }
            return new Operand("{ " + quote(DATE_PART_OPERATORS.get(range)) + ": " + strings.get(1) + "}");
        }

        private static Operand translateExtractQuarter(final Operand date) {
            final String extractQuarterFormatString =
                    "{'$cond': [{'$lte': [{'$month': %1$s}, 3]}, 1,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 6]}, 2,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 9]}, 3,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 12]}, 4,"
                            + " null]}]}]}]}";
            return Operand.format(extractQuarterFormatString, date);
        }

        public static Operand translateDayName(final RexCall rexCall, final List<Operand> strings) {
            final String dayNameFormatString =
                    " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 1]}, '%1$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 2]}, '%2$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 3]}, '%3$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 4]}, '%4$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 5]}, '%5$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 6]}, '%6$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 7]}, '%7$s',"
                            + " null]}]}]}]}]}]}]}";
            return Operand.format(dayNameFormatString,
                    DayOfWeek.SUNDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.MONDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.TUESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.WEDNESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.THURSDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.FRIDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.SATURDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    strings.get(0));
        }

        public static Operand translateMonthName(final RexCall rexCall, final List<Operand> strings) {
            final String monthNameFormatString =
                    "{'$cond': [{'$eq': [{'$month': %13$s}, 1]}, '%1$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 2]}, '%2$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 3]}, '%3$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 4]}, '%4$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 5]}, '%5$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 6]}, '%6$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 7]}, '%7$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 8]}, '%8$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 9]}, '%9$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 10]}, '%10$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 11]}, '%11$s',"
                    + " {'$cond': [{'$eq': [{'$month': %13$s}, 12]}, '%12$s',"
                    + " null]}]}]}]}]}]}]}]}]}]}]}]}";
            return Operand.format(monthNameFormatString,
                    Month.JANUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.FEBRUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.MARCH.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.APRIL.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.MAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.JUNE.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.JULY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.AUGUST.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.SEPTEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.OCTOBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.NOVEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    Month.DECEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    strings.get(0));
        }

        @SneakyThrows
        private static Operand translateFloor(final RexCall rexCall, final List<Operand> strings) {
            // TODO: Add support for integer floor with one operand
            if (rexCall.operands.size() != 2) {
                return null;
            }

            // NOTE: Required for getting FLOOR of date-time
            final RexNode operand2 = rexCall.operands.get(1);
            if (!(operand2.isA(SqlKind.LITERAL)
                    && operand2.getType().getSqlTypeName() == SqlTypeName.SYMBOL
                    && (((RexLiteral) operand2).getValue() instanceof TimeUnitRange))) {
                return null;
            }
            final RexLiteral literal = (RexLiteral) operand2;
            final TimeUnitRange timeUnitRange = literal.getValueAs(TimeUnitRange.class);
            switch (timeUnitRange) {
                case YEAR:
                case MONTH:
                    return new Operand(formatYearMonthFloorOperation(strings, timeUnitRange));
                case QUARTER:
                    return new Operand(formatQuarterFloorOperation(strings));
                case WEEK:
                case DAY:
                case HOUR:
                case MINUTE:
                case SECOND:
                case MILLISECOND:
                    return formatMillisecondFloorOperation(strings, timeUnitRange);
                default:
                    throw SqlError.createSQLFeatureNotSupportedException(LOGGER,
                            SqlError.UNSUPPORTED_PROPERTY, timeUnitRange.toString());
            }
        }

        private static String formatYearMonthFloorOperation(
                final List<Operand> strings,
                final TimeUnitRange timeUnitRange) {
            final String monthFormat = timeUnitRange == TimeUnitRange.YEAR ? "01" : "%m";
            return formatYearMonthFloorOperation(strings.get(0), monthFormat);
        }

        private static String formatYearMonthFloorOperation(
                final Operand dateOperand,
                final String monthFormat) {
            final String yearFormat = "%Y";
            return String.format(
                    "{'$dateFromString': {'dateString':"
                            + " {'$dateToString':"
                            + " {'date': %1$s, 'format': '%2$s-%3$s-01T00:00:00Z'}}}}",
                    dateOperand, yearFormat, monthFormat);
        }

        private static Operand formatMillisecondFloorOperation(
                final List<Operand> strings,
                final TimeUnitRange timeUnitRange) throws SQLFeatureNotSupportedException {

            final Instant baseDate = timeUnitRange == TimeUnitRange.WEEK
                    ? FIRST_DAY_OF_WEEK_AFTER_EPOCH // Monday (or first day of week)
                    : Instant.EPOCH;
            final long divisorLong = getDivisorValueForNumericFloor(timeUnitRange);
            final String divisor = String.format(
                    "{\"$numberLong\": \"%d\"}", divisorLong);
            final String subtract = String.format(
                    "{\"$subtract\": [%s, {\"$date\": {\"$numberLong\": \"%d\"}}]}",
                    strings.get(0), baseDate.toEpochMilli());
            final Operand divide = getIntegerDivisionOperation(subtract, divisor);
            final String multiply =  String.format(
                    "{\"$multiply\": [%s, %s]}", divisor, divide);
            return Operand.format(
                    "{\"$add\": [{\"$date\": {\"$numberLong\": \"%d\"}}, %s]}",
                    baseDate.toEpochMilli(), multiply);
        }

        private static String formatQuarterFloorOperation(final List<Operand> strings) {
            final String truncateQuarterFormatString =
                    "{'$cond': [{'$lte': [{'$month': %1$s}, 3]}, %2$s,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 6]}, %3$s,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 9]}, %4$s,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 12]}, %5$s,"
                            + " null]}]}]}]}";
            final String monthFormatJanuary = "01";
            final String monthFormatApril = "04";
            final String monthFormatJuly = "07";
            final String monthFormatOctober = "10";
            return String.format(truncateQuarterFormatString,
                    strings.get(0),
                    formatYearMonthFloorOperation(strings.get(0), monthFormatJanuary),
                    formatYearMonthFloorOperation(strings.get(0), monthFormatApril),
                    formatYearMonthFloorOperation(strings.get(0), monthFormatJuly),
                    formatYearMonthFloorOperation(strings.get(0), monthFormatOctober));
        }

        private static long getDivisorValueForNumericFloor(final TimeUnitRange timeUnitRange)
                throws SQLFeatureNotSupportedException {
            final long divisorLong;
            switch (timeUnitRange) {
                case WEEK:
                    divisorLong = ChronoUnit.WEEKS.getDuration().toMillis();
                    break;
                case DAY:
                    divisorLong = ChronoUnit.DAYS.getDuration().toMillis();
                    break;
                case HOUR:
                    divisorLong = ChronoUnit.HOURS.getDuration().toMillis();
                    break;
                case MINUTE:
                    divisorLong = ChronoUnit.MINUTES.getDuration().toMillis();
                    break;
                case SECOND:
                    divisorLong = ChronoUnit.SECONDS.getDuration().toMillis();
                    break;
                case MILLISECOND:
                    divisorLong = 1;
                    break;
                default:
                    throw SqlError.createSQLFeatureNotSupportedException(LOGGER,
                            SqlError.UNSUPPORTED_PROPERTY, timeUnitRange.toString());
            }
            return divisorLong;
        }
    }

    /**
     * Translates expressions using only query operators.
     */
    private static class SimpleMatchTranslator {
        private static final Map<SqlOperator, String> REVERSE_OPERATORS =
                new HashMap<>();
        static {
            REVERSE_OPERATORS.put(SqlStdOperatorTable.EQUALS, "$eq");
            REVERSE_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, "$ne");
            REVERSE_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, "$lte");
            REVERSE_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$lt");
            REVERSE_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, "$gte");
            REVERSE_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$gt");
        }

        private static String getObjectIdComparisonOperator(
                final RexCall call,
                final List<Operand> strings,
                final String stdOperator
        ) {
            // $or together the $oid and native operations.
            final String nativeOperation = getComparisonOperator(call, strings, stdOperator);
            final String oidOperation = getComparisonOperator(call, reformatObjectIdOperands(call, strings), stdOperator);
            if (nativeOperation != null && oidOperation != null) {
                return "{\"$or\": [" + oidOperation + ", " + nativeOperation + "]}";
            }
            return null;
        }

        private static String getComparisonOperator(
                final RexCall call,
                final List<Operand> strings,
                final String stdOperator) {
            // Handle NOT if the argument is a field reference.
            if (call.isA(SqlKind.NOT) && strings.get(0).isInputRef()) {
                return "{" + strings.get(0).getQueryValue() + ": false}";
            }
            // If given 2 arguments, attempt to do a binary comparison. Only 1 side can be a field reference.
            // The other side must be a literal or an expression that can be supported without aggregate operators.
            if (strings.size() == 2) {
                final Operand left = strings.get(0);
                final Operand right = strings.get(1);
                final String op = stdOperator;
                final String reverseOp = REVERSE_OPERATORS.get(call.getOperator());
                final String simpleComparison = formatSimpleBinaryComparison(op, left, right);
                if (simpleComparison != null) {
                    return simpleComparison;
                }
                // Try to return a simple comparison by swapping the operands or return null.
                return formatSimpleBinaryComparison(reverseOp, right, left);
            }
            // For any other scenario, return null.
            return null;
        }

        private static String getAndOrOperator(final List<Operand> operands, final String op) {
            final StringBuilder simple = new StringBuilder();
            simple.append("{").append(op).append(": [");
            for (Operand value: operands) {
                // If any operand is null, return null.
                if (value.getQueryValue() == null) {
                    return null;
                }
                simple.append(value.isInputRef() ? "{" + value.getQueryValue() + ": true}" : value.getQueryValue());
                simple.append(",");

            }
            simple.deleteCharAt(simple.length() - 1);
            simple.append("]}");
            return simple.toString();
        }

        private static String formatSimpleBinaryComparison(
                final String op,
                final Operand leftOperand,
                final Operand rightOperand
        ) {
            // If left side is field reference and the right side is not and neither query values are null,
            // return the simple comparison.
            if (leftOperand.isInputRef()
                    && rightOperand.isQuerySyntax()
                    && leftOperand.getQueryValue() != null
                    && rightOperand.getQueryValue() != null) {
                String comparison = "{" +  leftOperand.getQueryValue() + ": {" + op + ": " + rightOperand.getQueryValue() + "}}";

                // For not equals, need to also handle that value is not null or undefined.
                if (op.equals(RexToMongoTranslator.MONGO_OPERATORS.get(SqlStdOperatorTable.NOT_EQUALS))) {
                    comparison = "{" + leftOperand.getQueryValue() + ": {$nin: [null, " + rightOperand.getQueryValue() + "]}}";
                }
                return comparison;
            }
            return null;
        }

        private static String getNullCheckOperator(
                final RexCall call,
                final List<Operand> operands
        ) {
            final String op = call.getOperator() == SqlStdOperatorTable.IS_NULL ? "$eq" : "$ne";
            return operands.get(0).isInputRef()
                    ? "{" + operands.get(0).getQueryValue() + ": {" + op + ": null }}"
                    : null;
        }
    }

    /** Base class for planner rules that convert a relational expression to
     * MongoDB calling convention. */
    abstract static class DocumentDbConverterRule extends ConverterRule {
        protected DocumentDbConverterRule(final Config config) {
            super(config);
        }
    }

    /**
     * Rule to convert a {@link Sort} to a
     * {@link DocumentDbSort}.
     */
    private static class DocumentDbSortRule extends DocumentDbConverterRule {
        static final DocumentDbSortRule INSTANCE = Config.INSTANCE
                .withConversion(Sort.class, Convention.NONE, DocumentDbRel.CONVENTION,
                        "DocumentDbSortRule")
                .withRuleFactory(DocumentDbSortRule::new)
                .toRule(DocumentDbSortRule.class);

        DocumentDbSortRule(final Config config) {
            super(config);
        }

        @Override public RelNode convert(final RelNode rel) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet =
                    sort.getTraitSet().replace(out)
                            .replace(sort.getCollation());
            return new DocumentDbSort(rel.getCluster(), traitSet,
                    convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
                    sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    /**
     * Rule to convert a {@link LogicalFilter} to a
     * {@link DocumentDbFilter}.
     */
    private static class DocumentDbFilterRule extends DocumentDbConverterRule {
        static final DocumentDbFilterRule INSTANCE = Config.INSTANCE
                .withConversion(LogicalFilter.class, Convention.NONE,
                        DocumentDbRel.CONVENTION, "DocumentDbFilterRule")
                .withRuleFactory(DocumentDbFilterRule::new)
                .toRule(DocumentDbFilterRule.class);

        DocumentDbFilterRule(final Config config) {
            super(config);
        }

        @Override public RelNode convert(final RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace(out);
            return new DocumentDbFilter(
                    rel.getCluster(),
                    traitSet,
                    convert(filter.getInput(), out),
                    filter.getCondition());
        }
    }

    /**
     * Rule to convert a {@link LogicalProject}
     * to a {@link DocumentDbProject}.
     */
    private static class DocumentDbProjectRule extends DocumentDbConverterRule {
        static final DocumentDbProjectRule INSTANCE = Config.INSTANCE
                .withConversion(LogicalProject.class, Convention.NONE,
                        DocumentDbRel.CONVENTION, "DocumentDbProjectRule")
                .withRuleFactory(DocumentDbProjectRule::new)
                .toRule(DocumentDbProjectRule.class);

        DocumentDbProjectRule(final Config config) {
            super(config);
        }

        @Override public RelNode convert(final RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(out);
            return new DocumentDbProject(project.getCluster(), traitSet,
                    convert(project.getInput(), out), project.getProjects(),
                    project.getRowType());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalJoin} to
     * a {@link DocumentDbJoin}.
     */
    private static class DocumentDbJoinRule extends DocumentDbConverterRule {
        private static final DocumentDbJoinRule INSTANCE = Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE,
                        DocumentDbRel.CONVENTION, "DocumentDbJoinRule")
                .withRuleFactory(DocumentDbJoinRule::new)
                .toRule(DocumentDbJoinRule.class);

        protected DocumentDbJoinRule(final Config config) {
            super(config);
        }

        @Override public RelNode convert(final RelNode rel) {
            final LogicalJoin join = (LogicalJoin) rel;
            final RelTraitSet traitSet = join.getTraitSet().replace(out);
            return new DocumentDbJoin(join.getCluster(), traitSet,
                    convert(join.getLeft(), out),
                    convert(join.getRight(), out),
                    join.getCondition(), join.getJoinType());
        }
    }

    /*

    /**
     * Rule to convert a {@link LogicalCalc} to an
     * {@link MongoCalcRel}.
     o/
    private static class MongoCalcRule
        extends DocumentDbConverterRule {
      private MongoCalcRule(MongoConvention out) {
        super(
            LogicalCalc.class,
            Convention.NONE,
            out,
            "MongoCalcRule");
      }

      public RelNode convert(RelNode rel) {
        final LogicalCalc calc = (LogicalCalc) rel;

        // If there's a multiset, let FarragoMultisetSplitter work on it
        // first.
        if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
          return null;
        }

        return new MongoCalcRel(
            rel.getCluster(),
            rel.getTraitSet().replace(out),
            convert(
                calc.getChild(),
                calc.getTraitSet().replace(out)),
            calc.getProgram(),
            Project.Flags.Boxed);
      }
    }

    public static class MongoCalcRel extends SingleRel implements MongoRel {
      private final RexProgram program;

      /**
       * Values defined in {@link org.apache.calcite.rel.core.Project.Flags}.
       o/
      protected int flags;

      public MongoCalcRel(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode child,
          RexProgram program,
          int flags) {
        super(cluster, traitSet, child);
        assert getConvention() instanceof MongoConvention;
        this.flags = flags;
        this.program = program;
        this.rowType = program.getOutputRowType();
      }

      public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        return program.explainCalc(super.explainTerms(pw));
      }

      public double getRows() {
        return LogicalFilter.estimateFilteredRows(
            getChild(), program);
      }

      public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double dRows = RelMetadataQuery.getRowCount(this);
        double dCpu =
            RelMetadataQuery.getRowCount(getChild())
                * program.getExprCount();
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
      }

      public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MongoCalcRel(
            getCluster(),
            traitSet,
            sole(inputs),
            program.copy(),
            getFlags());
      }

      public int getFlags() {
        return flags;
      }

      public RexProgram getProgram() {
        return program;
      }

      public SqlString implement(MongoImplementor implementor) {
        final SqlBuilder buf = new SqlBuilder(implementor.dialect);
        buf.append("SELECT ");
        if (isStar(program)) {
          buf.append("*");
        } else {
          for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
            buf.append(ref.i == 0 ? "" : ", ");
            expr(buf, program, ref.e);
            alias(buf, null, getRowType().getFieldNames().get(ref.i));
          }
        }
        implementor.newline(buf)
            .append("FROM ");
        implementor.subQuery(buf, 0, getChild(), "t");
        if (program.getCondition() != null) {
          implementor.newline(buf);
          buf.append("WHERE ");
          expr(buf, program, program.getCondition());
        }
        return buf.toSqlString();
      }

      private static boolean isStar(RexProgram program) {
        int i = 0;
        for (RexLocalRef ref : program.getProjectList()) {
          if (ref.getIndex() != i++) {
            return false;
          }
        }
        return i == program.getInputRowType().getFieldCount();
      }

      private static void expr(
          SqlBuilder buf, RexProgram program, RexNode rex) {
        if (rex instanceof RexLocalRef) {
          final int index = ((RexLocalRef) rex).getIndex();
          expr(buf, program, program.getExprList().get(index));
        } else if (rex instanceof RexInputRef) {
          buf.identifier(
              program.getInputRowType().getFieldNames().get(
                  ((RexInputRef) rex).getIndex()));
        } else if (rex instanceof RexLiteral) {
          toSql(buf, (RexLiteral) rex);
        } else if (rex instanceof RexCall) {
          final RexCall call = (RexCall) rex;
          switch (call.getOperator().getSyntax()) {
          case Binary:
            expr(buf, program, call.getOperands().get(0));
            buf.append(' ')
                .append(call.getOperator().toString())
                .append(' ');
            expr(buf, program, call.getOperands().get(1));
            break;
          default:
            throw new AssertionError(call.getOperator());
          }
        } else {
          throw new AssertionError(rex);
        }
      }
    }

    private static SqlBuilder toSql(SqlBuilder buf, RexLiteral rex) {
      switch (rex.getTypeName()) {
      case CHAR:
      case VARCHAR:
        return buf.append(
            new NlsString(rex.getValue2().toString(), null, null)
                .asSql(false, false));
      default:
        return buf.append(rex.getValue2().toString());
      }
    }

     */

    /**
     * Rule to convert an {@link LogicalAggregate}
     * to an {@link DocumentDbAggregate}.
     */
    private static class DocumentDbAggregateRule extends DocumentDbConverterRule {
        static final DocumentDbAggregateRule INSTANCE = Config.INSTANCE
                .withConversion(LogicalAggregate.class, Convention.NONE,
                        DocumentDbRel.CONVENTION, "DocumentDbAggregateRule")
                .withRuleFactory(DocumentDbAggregateRule::new)
                .toRule(DocumentDbAggregateRule.class);

        DocumentDbAggregateRule(final Config config) {
            super(config);
        }

        @Override public RelNode convert(final RelNode rel) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet =
                    agg.getTraitSet().replace(out);
            try {
                return new DocumentDbAggregate(
                        rel.getCluster(),
                        traitSet,
                        convert(agg.getInput(), traitSet.simplify()),
                        agg.getGroupSet(),
                        agg.getGroupSets(),
                        agg.getAggCallList());
            } catch (InvalidRelException e) {
                LOGGER.warn(e.toString());
                return null;
            }
        }
    }

    /**
     * Container for operands with optional column metadata.
     */
    @Getter
    static class Operand {
        private final String aggregationValue;
        private final String queryValue;
        private final boolean isQuerySyntax;
        private final DocumentDbSchemaColumn column;

        /**
         * Constructs an Operand from a String value. Only aggregation value is set. All other values
         * will be null.
         *
         * @param aggregationValue the String value.
         */
        public Operand(final String aggregationValue) {
            this(aggregationValue, null, false, null);
        }

        /**
         * Constructs an Operand from the given values.The column value is left as null.
         *
         * @param aggregationValue the String value. This is an expression using aggregation operators.
         * @param queryValue This is an expression using query operators.
         * @param isQuerySyntax Whether or not the operand can be used on the right of query operators. This
         *     includes literals, literals wrapped in CAST, and simple scalar function calls such as
         *     CURRENT TIME.
         */
        public Operand(
                final String aggregationValue, final String queryValue, final boolean isQuerySyntax) {
            this(aggregationValue, queryValue, isQuerySyntax, null);
        }

        /**
         * Constructs an Operand from the given values.
         * @param aggregationValue the String value. This is an expression using aggregation operators.
         * @param queryValue This is an expression using query operators.
         * @param isQuerySyntax Whether or not the operand can be used on the right of query operators. This includes literals, literals wrapped in CAST,
         *                      and simple scalar function calls such as CURRENT TIME.
         * @param column Column metadata if the operand represents a field reference.
         */
        public Operand(
                final String aggregationValue,
                final String queryValue,
                final boolean isQuerySyntax,
                final DocumentDbSchemaColumn column) {
            this.aggregationValue = aggregationValue;
            this.queryValue = queryValue;
            this.isQuerySyntax = isQuerySyntax;
            this.column = column;
        }

        /**
         * Formats the string value using the {@link String#format(String, Object...)} method.
         *
         * @param format the format string.
         * @param args the optional arguments.
         * @return the Operand with the value formatted.
         */
        public static Operand format(final String format, final Object... args) {
            return new Operand(String.format(format, args));
        }

        /**
         * Checks if operand represents a field reference. This includes RexInputRef as well as
         * field references wrapped in calls to CAST or REINTERPRET.
         */
        public boolean isInputRef() {
            return this.column != null;
        }

        @Override
        public String toString() {
            return aggregationValue;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof String) {
                final String stringValue = (String) o;
                return stringValue.equals(this.getAggregationValue());
            }
            if (!(o instanceof Operand)) {
                return false;
            }
            final Operand that = (Operand) o;
            return Objects.equals(getAggregationValue(), that.getAggregationValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getAggregationValue());
        }
    }

    /*
    /**
     * Rule to convert an {@link org.apache.calcite.rel.logical.Union} to a
     * {@link MongoUnionRel}.
     o/
    private static class MongoUnionRule
        extends DocumentDbConverterRule {
      private MongoUnionRule(MongoConvention out) {
        super(
            Union.class,
            Convention.NONE,
            out,
            "MongoUnionRule");
      }

      public RelNode convert(RelNode rel) {
        final Union union = (Union) rel;
        final RelTraitSet traitSet =
            union.getTraitSet().replace(out);
        return new MongoUnionRel(
            rel.getCluster(),
            traitSet,
            convertList(union.getInputs(), traitSet),
            union.all);
      }
    }

    public static class MongoUnionRel
        extends Union
        implements MongoRel {
      public MongoUnionRel(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          List<RelNode> inputs,
          boolean all) {
        super(cluster, traitSet, inputs, all);
      }

      public MongoUnionRel copy(
          RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MongoUnionRel(getCluster(), traitSet, inputs, all);
      }

      @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.1);
      }

      public SqlString implement(MongoImplementor implementor) {
        return setOpSql(this, implementor, "UNION");
      }
    }

    private static SqlString setOpSql(
        SetOp setOpRel, MongoImplementor implementor, String op) {
      final SqlBuilder buf = new SqlBuilder(implementor.dialect);
      for (Ord<RelNode> input : Ord.zip(setOpRel.getInputs())) {
        if (input.i > 0) {
          implementor.newline(buf)
              .append(op + (setOpRel.all ? " ALL " : ""));
          implementor.newline(buf);
        }
        buf.append(implementor.visitChild(input.i, input.e));
      }
      return buf.toSqlString();
    }

    /**
     * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalIntersect}
     * to an {@link MongoIntersectRel}.
     o/
    private static class MongoIntersectRule
        extends DocumentDbConverterRule {
      private MongoIntersectRule(MongoConvention out) {
        super(
            LogicalIntersect.class,
            Convention.NONE,
            out,
            "MongoIntersectRule");
      }

      public RelNode convert(RelNode rel) {
        final LogicalIntersect intersect = (LogicalIntersect) rel;
        if (intersect.all) {
          return null; // INTERSECT ALL not implemented
        }
        final RelTraitSet traitSet =
            intersect.getTraitSet().replace(out);
        return new MongoIntersectRel(
            rel.getCluster(),
            traitSet,
            convertList(intersect.getInputs(), traitSet),
            intersect.all);
      }
    }

    public static class MongoIntersectRel
        extends Intersect
        implements MongoRel {
      public MongoIntersectRel(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          List<RelNode> inputs,
          boolean all) {
        super(cluster, traitSet, inputs, all);
        assert !all;
      }

      public MongoIntersectRel copy(
          RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MongoIntersectRel(getCluster(), traitSet, inputs, all);
      }

      public SqlString implement(MongoImplementor implementor) {
        return setOpSql(this, implementor, " intersect ");
      }
    }

    /**
     * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalMinus}
     * to an {@link MongoMinusRel}.
     o/
    private static class MongoMinusRule
        extends DocumentDbConverterRule {
      private MongoMinusRule(MongoConvention out) {
        super(
            LogicalMinus.class,
            Convention.NONE,
            out,
            "MongoMinusRule");
      }

      public RelNode convert(RelNode rel) {
        final LogicalMinus minus = (LogicalMinus) rel;
        if (minus.all) {
          return null; // EXCEPT ALL not implemented
        }
        final RelTraitSet traitSet =
            rel.getTraitSet().replace(out);
        return new MongoMinusRel(
            rel.getCluster(),
            traitSet,
            convertList(minus.getInputs(), traitSet),
            minus.all);
      }
    }

    public static class MongoMinusRel
        extends Minus
        implements MongoRel {
      public MongoMinusRel(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          List<RelNode> inputs,
          boolean all) {
        super(cluster, traitSet, inputs, all);
        assert !all;
      }

      public MongoMinusRel copy(
          RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MongoMinusRel(getCluster(), traitSet, inputs, all);
      }

      public SqlString implement(MongoImplementor implementor) {
        return setOpSql(this, implementor, " minus ");
      }
    }

    public static class MongoValuesRule extends DocumentDbConverterRule {
      private MongoValuesRule(MongoConvention out) {
        super(
            LogicalValues.class,
            Convention.NONE,
            out,
            "MongoValuesRule");
      }

      @Override public RelNode convert(RelNode rel) {
        LogicalValues valuesRel = (LogicalValues) rel;
        return new MongoValuesRel(
            valuesRel.getCluster(),
            valuesRel.getRowType(),
            valuesRel.getTuples(),
            valuesRel.getTraitSet().plus(out));
      }
    }

    public static class MongoValuesRel
        extends Values
        implements MongoRel {
      MongoValuesRel(
          RelOptCluster cluster,
          RelDataType rowType,
          List<List<RexLiteral>> tuples,
          RelTraitSet traitSet) {
        super(cluster, rowType, tuples, traitSet);
      }

      @Override public RelNode copy(
          RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MongoValuesRel(
            getCluster(), rowType, tuples, traitSet);
      }

      public SqlString implement(MongoImplementor implementor) {
        throw new AssertionError(); // TODO:
      }
    }
    */
}
