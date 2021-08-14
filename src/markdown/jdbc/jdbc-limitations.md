# JDBC Support & Limitations

## Schema, Catalog, and Tables
The semantics of schema, catalogs, and tables often vary by database vendor.
For the DocumentDB JDBC driver, the "schema" is a particular database on a DocumentDB cluster, and
the "tables" are determined from the collections within that database through [schema discovery]().
There is no concept of "catalogs". A user can only work within 1 database 
at a time even though they might have access to others.

## Interface Limitations
This section highlights limitations to keep in mind with some of the JDBC interfaces 
implemented by the driver.

## CallableStatement
`CallableStatement` and the use of stored procedures in general is not supported.

## Connection
The driver does not support transactions of any kind. 
As such, this limits the methods available on a `Connection`.

## DatabaseMetaData
Of the JDBC API's `DatabaseMetaData` [methods](https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html),
the following are unsupported:

- `getBestRowIdentifier()`
- `getClientInfoProperties()`
- `getCrossReference()`
- `getExportedKeys()`
- `getFunctionColumns()`
- `getFunctions()`
- `getIndexInfo()`
- `getProcedureColumns()`
- `getPseudoColumns()`
- `getSuperTables()`
- `getSuperTypes()`
- `getTablePrivileges`
- `getTypeInfo()`
- `getUDTs()`
- `getVersionColumns()`

When called, these methods will throw a `SqlException`.

## PreparedStatement
To support BI tools that may use the `PreparedStatement` interface in auto-generated queries, the driver  
supports the use of `PreparedStatement`. However, the use of parameters (values left as `?`) is not supported
and repeated calls to execute a `PreparedStatement` do not have a reduced parsing or query time.
This implementation has no significant differences with the `Statement` interface.

Of the JDBC API's `PreparedStatement` [methods](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html),
the following are unsupported:

- `addBatch()`
- `clearParameters()`
- `executeLargeUpdate()`
- `executeUpdate()`
- `getParameterMetaData()`
- any set parameter method such as `setArray(int parameterIndex, Array x)`

When called, these methods will throw a `SqlException`.

## ResultSet
Every `ResultSet` returned by the driver will have a read-only concurrency mode,
a forward fetch direction, and a forward-only cursor.
As such, this limits the methods available on a `ResultSet`.
Of the JDBC API's `ResultSet` [methods](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html),
the following are unsupported or supported with some limitations:

#### Unsupported methods
When called, these methods will throw a `SqlException`:

- `afterLast()`
- `beforeFirst()`
- `cancelRowUpdates()`
- `deleteRow()`
- `first()`
- `insertRow()`
- `last()`
- `moveToCurrentRow()`
- `moveToInsertRow()`
- `previous()`
- `refreshRow()`
- `rowDeleted()`
- `rowInserted()`
- `rowUpdated()`
- `getUnicodeStream()`
- `getRef()`
- `getArray()`
- `getURL()`
- `getRowId()`
- `getSQLXML()`
- any update method for all data types such as `updateBlob(int columnLabel, Blob x)`

### Supported with limitations
These methods only accept certain inputs. When called with an invalid input, these will throw a `SqlException`.
- `absolute(int row)` - This will only accept positive values that are greater or equal to the current row.
- `relative(int rows)` - This only accepts positive values as the cursor only moves forward.
- `setFetchDirection(int direction)` - This only accepts setting the direction to `FETCH_FORWARD` which would be
  a no-op since this is already the default direction.
  
## Statement
Of the JDBC API's `Statement` [methods](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html),
the following are unsupported:

- `addBatch()`
- `clearBatch()`
- `executeBatch()`
- `executeLargeBatch()`
- `executeLargeUpdate()`
- `executeUpdate()`
- `setPoolable()`

When called, these methods will throw a `SqlException`.