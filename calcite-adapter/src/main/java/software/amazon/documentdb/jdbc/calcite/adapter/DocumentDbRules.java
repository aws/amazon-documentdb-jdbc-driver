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

import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable;
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
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbMetadataColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.sql.SQLFeatureNotSupportedException;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.AbstractList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * Rules and relational operators for
 * {@link DocumentDbRel#CONVENTION MONGO}
 * calling convention.
 */
public final class DocumentDbRules {
    private DocumentDbRules() { }

    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

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
        if (column.isIndex()) {
            path = column.getSqlName();
        } else if (column instanceof DocumentDbMetadataColumn
                && (!isNullOrWhitespace(((DocumentDbMetadataColumn) column).getResolvedPath())
                && !useOriginalPaths)) {
            path = ((DocumentDbMetadataColumn) column).getResolvedPath();
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
    static class RexToMongoTranslator extends RexVisitorImpl<String> {
        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;

        private static final Map<SqlOperator, String> MONGO_OPERATORS =
                new HashMap<>();
        private static final Map<SqlOperator,
                BiFunction<RexCall, List<String>, String>> REX_CALL_TO_MONGO_MAP = new HashMap<>();

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

            // Arithmetic
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.DIVIDE,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.MULTIPLY,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.MOD,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.PLUS,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.MINUS,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.MINUS_DATE,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.DIVIDE_INTEGER,
                    RexToMongoTranslator::getMongoAggregateForIntegerDivide);
            // Boolean
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.AND,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.OR,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.NOT,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            // Comparison
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.EQUALS,
                    (call, strings) -> getMongoAggregateForOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            // Need to handle null value
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.NOT_EQUALS,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.GREATER_THAN,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.LESS_THAN,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    (call, strings) -> getMongoAggregateForComparisonOperator(
                            call, strings, MONGO_OPERATORS.get(call.getOperator())));

            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.IS_NULL,
                    RexToMongoTranslator::getMongoForNullOperator);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.IS_NOT_NULL,
                    RexToMongoTranslator::getMongoForNullOperator);

            // Date operations
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.CURRENT_DATE, DateFunctionTranslator::translateCurrentTimestamp);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.CURRENT_TIME, DateFunctionTranslator::translateCurrentTimestamp);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.CURRENT_TIMESTAMP, DateFunctionTranslator::translateCurrentTimestamp);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.DATETIME_PLUS, DateFunctionTranslator::translateDateAdd);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.EXTRACT, DateFunctionTranslator::translateExtract);
            REX_CALL_TO_MONGO_MAP.put(SqlLibraryOperators.DAYNAME, DateFunctionTranslator::translateDayName);
            REX_CALL_TO_MONGO_MAP.put(SqlLibraryOperators.MONTHNAME, DateFunctionTranslator::translateMonthName);
            // CASE, ITEM
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.CASE, RexToMongoTranslator::getMongoAggregateForCase);
            REX_CALL_TO_MONGO_MAP.put(SqlStdOperatorTable.ITEM, RexToMongoTranslator::getMongoAggregateForItem);

        }

        protected RexToMongoTranslator(final JavaTypeFactory typeFactory,
                final List<String> inFields) {
            super(true);
            this.typeFactory = typeFactory;
            this.inFields = inFields;
        }

        @Override public String visitLiteral(final RexLiteral literal) {
            if (literal.getValue() == null) {
                return "null";
            }

            switch (literal.getType().getSqlTypeName()) {
                case DOUBLE:
                case DECIMAL:
                    return "{\"$numberDouble\": \"" + literal.getValueAs(Double.class) + "\"}";
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_HOUR:
                case INTERVAL_MINUTE:
                case INTERVAL_SECOND:
                    // Convert supported intervals to milliseconds.
                    return "{\"$numberLong\": \"" + literal.getValueAs(Long.class) + "\"}";
                case DATE:
                    return "{\"$date\": {\"$numberLong\": \"" + literal.getValueAs(Integer.class) + "\" } }";
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // Convert from date in milliseconds to MongoDb date.
                    return "{\"$date\": {\"$numberLong\": \"" + literal.getValueAs(Long.class) + "\" } }";
                default:
                    /*
                    TODO: AD-239: Re-add use of literal here.
                    return "{\"$literal\": "
                            + RexToLixTranslator.translateLiteral(literal, literal.getType(),
                            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE)
                            + "}";

                     */
                    return RexToLixTranslator.translateLiteral(literal, literal.getType(),
                            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE).toString();
            }
        }

        @Override public String visitInputRef(final RexInputRef inputRef) {
            return maybeQuote(
                    "$" + inFields.get(inputRef.getIndex()));
        }

        @SneakyThrows
        @Override public String visitCall(final RexCall call) {
            final String name = isItem(call);
            if (name != null) {
                return "'$" + name + "'";
            }

            final List<String> strings = visitList(call.operands);
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.REINTERPRET) {
                // TODO: Handle case when DocumentDB supports $convert.
                return strings.get(0);
            }

            if (REX_CALL_TO_MONGO_MAP.containsKey(call.getOperator())) {
                final String result = REX_CALL_TO_MONGO_MAP.get(call.getOperator()).apply(call, strings);
                if (result != null) {
                    return result;
                }
            }

            throw new IllegalArgumentException("Translation of " + call
                    + " is not supported by DocumentDbRules");
        }

        private static String getMongoAggregateForIntegerDivide(final RexCall call, final List<String> strings) {
            // TODO: when $trunc is supported in DocumentDB, add back.
            //final String intDivideOptFormat = "{ \"$trunc\": [ {\"$divide\": [%s]}, 0 ]}";
            final String intDivideOptFormat = "{\"$divide\": [%s]}";
            return String.format(intDivideOptFormat, Util.commaList(strings));
        }

        private static String getMongoAggregateForCase(
                final RexCall call,
                final List<String> strings) {
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
            return sb.toString();
        }

        private static String getMongoAggregateForItem(
                final RexCall call,
                final List<String> strings) {
            final RexNode op1 = call.operands.get(1);
            if (op1 instanceof RexLiteral
                    && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                if (!Bug.CALCITE_194_FIXED) {
                    return "'" + stripQuotes(strings.get(0)) + "["
                            + ((RexLiteral) op1).getValue2() + "]'";
                }
                return strings.get(0) + "[" + strings.get(1) + "]";
            }
            return null;
        }

        @SneakyThrows
        private static String getMongoAggregateForComparisonOperator(
                final RexCall call,
                final List<String> strings,
                final String stdOperator) {
            final String op = getMongoAggregateForOperator(call, strings, stdOperator);
            return addNullChecksToQuery(strings, op);
        }

        @SneakyThrows
        private static String getMongoAggregateForOperator(
                final RexCall call,
                final List<String> strings,
                final String stdOperator) {
            verifySupportedType(call);
            return "{" + maybeQuote(stdOperator) + ": [" + Util.commaList(strings) + "]}";
        }

        @SneakyThrows
        private static String getMongoForNullOperator(final RexCall call, final List<String> strings) {
            if (strings.size() != 1) {
                throw SqlError.createSQLException(LOGGER,
                        SqlState.INVALID_QUERY_EXPRESSION,
                        SqlError.INVALID_QUERY,
                        String.format("IS [NOT] NULL should have one argument, received %d",
                                strings.size()));
            }
            if (call.getOperator().equals(SqlStdOperatorTable.IS_NULL)) {
                return "{$lte: [" + strings.get(0) + ", null]}";
            }
            if (call.getOperator().equals(SqlStdOperatorTable.IS_NOT_NULL)) {
                return "{$gt: [" + strings.get(0) + ", null]}";
            }
            return null;
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

        private static String addNullChecksToQuery(final List<String> strings, final String op) {
            final StringBuilder sb = new StringBuilder("{\"$and\": [");
            sb.append(op);
            for (int i = 0; i < 2; i++) {
                if (!strings.get(i).equals("null")) {
                    // The operator {$gt null} filters out any values that are null or undefined.
                    sb.append(",{\"$gt\": [");
                    sb.append(strings.get(i));
                    sb.append(", null]}");
                }
            }
            sb.append("]}");
            return sb.toString();
        }

    }

    private static String stripQuotes(final String s) {
        return s.startsWith("'") && s.endsWith("'")
                ? s.substring(1, s.length() - 1)
                : s;
    }

    private static class DateFunctionTranslator {

        private static final Map<TimeUnitRange, String> DATE_PART_OPERATORS =
                new HashMap<>();

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

        private static String translateCurrentTimestamp(final RexCall rexCall, final List<String> strings) {
            return "new Date()";
        }

        private static String translateDateAdd(final RexCall call, final List<String> strings) {
            // TODO: Check for unsupported intervals and throw error/emulate in some other way.
            return "{ \"$add\":" + "[" + Util.commaList(strings) + "]}";
        }

        private static String translateExtract(final RexCall call, final List<String> strings) {
            // The first argument to extract is the interval (literal)
            // and the second argument is the date (can be any node evaluating to a date).
            final RexLiteral literal = (RexLiteral) call.getOperands().get(0);
            final TimeUnitRange range = literal.getValueAs(TimeUnitRange.class);

            // TODO: Check for unsupported time unit (ex: quarter) and emulate in some other way.
            if (range == TimeUnitRange.QUARTER) {
                return translateExtractQuarter(strings);
            }
            return "{ " + quote(DATE_PART_OPERATORS.get(range)) + ": " + strings.get(1) + "}";
        }

        private static String translateExtractQuarter(final List<String> strings) {
            final String extractQuarterFormatString =
                    "{'$cond': [{'$lte': [{'$month': %1$s}, 3]}, 1,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 6]}, 2,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 9]}, 3,"
                            + " {'$cond': [{'$lte': [{'$month': %1$s}, 12]}, 4,"
                            + " null]}]}]}]}";
            return String.format(extractQuarterFormatString, strings.get(1));
        }

        public static String translateDayName(final RexCall rexCall, final List<String> strings) {
            final String dayNameFormatString =
                    " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 1]}, '%1$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 2]}, '%2$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 3]}, '%3$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 4]}, '%4$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 5]}, '%5$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 6]}, '%6$s',"
                            + " {'$cond': [{'$eq': [{'$dayOfWeek': %8$s}, 7]}, '%7$s',"
                            + " null]}]}]}]}]}]}]}";
            return String.format(dayNameFormatString,
                    DayOfWeek.SUNDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.MONDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.TUESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.WEDNESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.THURSDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.FRIDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    DayOfWeek.SATURDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    strings.get(0));
        }

        public static String translateMonthName(final RexCall rexCall, final List<String> strings) {
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
            return String.format(monthNameFormatString,
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
