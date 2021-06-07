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

package software.amazon.documentdb.jdbc.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import java.sql.SQLException;

/**
 * Abstract implementation of ResultSetMetaData for JDBC Driver.
 */
public abstract class ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetMetaData.class);

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return (null != iface) && iface.isAssignableFrom(this.getClass());
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        }

        throw SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.CANNOT_UNWRAP,
                iface.toString());
    }
}
