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

package software.amazon.documentdb.jdbc.common.utilities;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.beanutils.converters.AbstractConverter;
import org.apache.commons.beanutils.converters.ArrayConverter;
import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.commons.beanutils.converters.BooleanConverter;
import org.apache.commons.beanutils.converters.ByteConverter;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.DateTimeConverter;
import org.apache.commons.beanutils.converters.DoubleConverter;
import org.apache.commons.beanutils.converters.FloatConverter;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import org.apache.commons.beanutils.converters.NumberConverter;
import org.apache.commons.beanutils.converters.ShortConverter;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.commons.beanutils.converters.StringConverter;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Provides a map of type converters.
 */
public class TypeConverters {
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConverters.class);
    private static final ImmutableMap<Class<?>, AbstractConverter> TYPE_CONVERTERS_MAP;

    static {
        TYPE_CONVERTERS_MAP = ImmutableMap.<Class<?>, AbstractConverter>builder()
                .put(Decimal128.class, new Decimal128Converter(new Decimal128(0)))
                .put(BigDecimal.class, new BigDecimalConverter(0))
                .put(Boolean.class, new BooleanConverter(false))
                .put(boolean.class, new BooleanConverter(false))
                .put(BsonTimestamp.class, new BsonTimestampConverter())
                .put(BsonRegularExpression.class, new StringConverter())
                .put(Byte.class, new ByteConverter(0))
                .put(byte.class, new ByteConverter(0))
                .put(Date.class, new DateConverter(null))
                .put(java.sql.Date.class, new DateConverter(null))
                .put(Double.class, new DoubleConverter(0.0))
                .put(double.class, new DoubleConverter(0.0))
                .put(Float.class, new FloatConverter(0.0))
                .put(float.class, new FloatConverter(0.0))
                .put(Integer.class, new IntegerConverter(0))
                .put(int.class, new IntegerConverter(0))
                .put(Long.class, new LongConverter(0))
                .put(long.class, new LongConverter(0))
                .put(MaxKey.class, new StringConverter())
                .put(MinKey.class, new StringConverter())
                .put(ObjectId.class, new StringConverter())
                .put(Short.class, new ShortConverter(0))
                .put(short.class, new ShortConverter(0))
                .put(String.class, new StringConverter())
                .put(Timestamp.class, new SqlTimestampConverter())
                .put(Byte[].class, new ArrayConverter(Byte[].class, new ByteConverter(), -1))
                .put(byte[].class, new ArrayConverter(byte[].class, new ByteConverter(), -1))
                .build();
    }

    /**
     * Gets the type converter for the given source type.
     *
     * @param sourceType the source type to get the converter for.
     * @param targetType the target type used to log error in case of missing converter.
     * @return a {@link AbstractConverter} instance for the source type.
     *
     * @throws SQLException if a converter cannot be found the source type.
     */
    public static AbstractConverter get(final Class<? extends Object> sourceType,
            final Class<? extends Object> targetType) throws SQLException {
        final AbstractConverter converter = TYPE_CONVERTERS_MAP.get(sourceType);
        if (converter == null) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_CONVERSION,
                    sourceType.getSimpleName(),
                    targetType.getSimpleName());
        }
        return converter;
    }

    /**
     * Converter for Decimal128 type.
     */
    private static class Decimal128Converter extends NumberConverter {
        /**
         * Default constructor for converter.
         */
        public Decimal128Converter() {
            super(true);
        }

        /**
         * Constuctor for converter where you can specify the default value.
         * @param defaultValue the default value for conversion.
         */
        public Decimal128Converter(final Object defaultValue) {
            super(true, defaultValue);
        }

        /**
         * Converts to the target type. Specifically tries to handle conversion from {@link Decimal128} to
         * type{@link BigDecimal}.
         *
         * @param targetType Data type to which this value should be converted.
         * @param value The input value to be converted.
         * @return a value converted to the target type or the value.
         * @param <T> the type of return value.
         * @throws Throwable thrown on conversion exception.
         */
        @Override
        protected <T> T convertToType(final Class<T> targetType, final Object value) throws Throwable {
            if (value instanceof Decimal128) {
                if (targetType.isAssignableFrom(BigDecimal.class)) {
                    return targetType.cast(((Decimal128) value).bigDecimalValue());
                }
                return super.convertToType(targetType, ((Decimal128) value).doubleValue());
            }
            return super.convertToType(targetType, value);
        }

        @Override
        protected String convertToString(final Object value) throws Throwable {
            if (value instanceof Decimal128) {
                return ((Decimal128) value).toString();
            }
            return super.convertToString(value);
        }

        @Override
        protected Class<?> getDefaultType() {
            return Decimal128.class;
        }
    }

    private static class BsonTimestampConverter extends DateTimeConverter {

        /**
         * Creates a {@link BsonTimestampConverter} with no default value.
         */
        public BsonTimestampConverter() {
            super();
        }

        /**
         * Creates a {@link BsonTimestampConverter} with a default value.
         *
         * @param defaultValue the default value if source value missing or cannot be converted.
         */
        public BsonTimestampConverter(final Object defaultValue) {
            super(defaultValue);
        }

        @Override
        protected <T> T convertToType(final Class<T> targetType, final Object value) throws Exception {
            if (value instanceof BsonTimestamp) {
                // This returns time in seconds since epoch.
                final int timeInSecsSinceEpoch = ((BsonTimestamp) value).getTime();
                return super.convertToType(targetType, TimeUnit.SECONDS.toMillis(timeInSecsSinceEpoch));
            }
            return super.convertToType(targetType, value);
        }

        @Override
        protected String convertToString(final Object value) throws Throwable {
            if (value instanceof BsonTimestamp) {
                return super.convertToString(((BsonTimestamp) value).getValue());
            }
            return super.convertToString(value);
        }

        @Override
        protected Class<?> getDefaultType() {
            return BsonTimestamp.class;
        }
    }
}
