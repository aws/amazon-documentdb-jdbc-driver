# Support and Limitations

## JDBC
The DocumentDB JDBC Driver implements the [JDBC 4.2 API]() and users can typically rely on its documentation 
when using the driver although there are some limitations. These are documented in this section. 

### DatabaseMetaData   

#### Schema, Catalog, and Tables
The semantics of schema, catalogs, and tables often vary by database vendor.
For the DocumentDB JDBC driver, the "schema" is a particular database on a DocumentDB cluster, and 
the "tables" are determined from the collections within that database through [schema discovery](). 
There is no concept of "catalogs" and calls to the `getCatalogs` method in `DatabaseMetaData` will 
return `null`. When connecting to a DocumentDB cluster, a user can only work within 1 database at a time 
even though they might have access to others.

### ResultSet
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

### PreparedStatement
To support BI tools that may use the `PreparedStatement` interface in auto-generated queries, the driver  
supports the use of `PreparedStatement`. However, the use of parameters (values left as `?`) is not supported 
and repeated calls to execute a `PreparedStatement` do not have a reduced parsing or query time. 
This implementation has no significant differences with the `Statement` interface. 

### CallableStatement 
`CallableStatement` and the use of stored procedures in general is not supported.

## SQL
This section takes some syntax and grammar definitions from the documentation of the 
[Apache Calcite]() project.

### Data Definition Language (DDL)
The driver is read-only and as such does not support any kind of DDL statements. 

### Basic Query Format 
The driver supports `SELECT` statements of the general form (using BNF notation): 
```
SELECT [ ALL | DISTINCT ]
   { * | projectItem [, projectItem ]* }
   FROM tableExpression
   [ WHERE booleanExpression ]
   [ GROUP BY { groupItem [, groupItem ]* } ]
   [ HAVING booleanExpression ]
   [ ORDER BY orderItem [, orderItem ]* ]
   [ LIMIT number ]
   [ OFFSET start { ROW | ROWS } ]

projectItem:
   expression [ [ AS ] columnAlias ] |   tableAlias . *
   
orderItem:
   expression [ ASC | DESC ]
   
groupItem:
   expression

tableExpression:
   tableReference [, tableReference ]*
   |   tableExpression [ NATURAL | INNER | LEFT OUTER | CROSS ] JOIN tableExpression [ joinCondition ]
      
joinCondition:
   ON booleanExpression

tableReference: 
   [ schemaName . ] tableName  
```
Queries without a `FROM` clause or only using `VALUES` in the `FROM` clause are not supported.
A query must specify 1 or more tables.

Set operations such as `UNION`, `INTERSECT` and `MINUS` are not supported.
Grouping operations using `CUBE`, `ROLLLUP` or `GROUPING SETS` are not supported.
Ordering using `NULLS FIRST` or `NULLS LAST` is not supported.

### Identifiers
Identifiers are the names of tables and columns in a SQL query.  

Quoting is optional but unquoted identifiers must start with a letter 
and can only contain letters, digits, and underscores. 
Quoted identifiers start and end with double quotes. 
They may contain virtually any character. If you wish to include a double quote in an identifier, 
use another double quote to escape it.
The maximum identifier length, quoted or unquoted, is 128 characters.

Identifier matching is case-sensitive and identifiers that match a reserved SQL keyword must be quoted 
or use fully qualified names.

### Joins

#### Cross Collection Joins

Currently, cross collection joins are not supported.

#### Same Collection Joins

Currently, the driver only supports `JOINs` across tables from the same collection as long as we
are only joining on foreign keys. This is equivalent to presenting the data in its denormalized
form. For such `JOINs`, the complete set of foreign keys for a table must be used.

For example, if we had the collection `Customer` whose documents roughly followed the form below, we
would end up with 4 tables.

```json
  {
    "_id": "112244",
    "name": "Joan Starr",
    "address": {
      "street": "123 A Street", 
      "postal": "12345"
    },
    "subscriptions": [
      {
        "magazine": "Vogue", 
        "variants": [ "UK", "US" ]
      },
      {
        "magazine": "Tattle",
        "variants": [ "Singapore", "UK" ]
      }
    ]
  } 
 ```

#### Table: customer

| Column Name | Data Type | Key |
| --- | --- | --- |
| _**customer__id**_ | VARCHAR | PK |
| name | VARCHAR | |

#### Table: customer_address

| Column Name | Data Type | Key |
| --- | --- | --- |
| _**customer__id**_ | VARCHAR | PK/FK |
| street | VARCHAR | |
| postal | VARCHAR | |

#### Table: customer_subscriptions

| Column Name | Data Type | Key |
| --- | --- | --- |
| _**customer__id**_ | VARCHAR | PK/FK |
| subscriptions_index_lvl_0 | BIGINT | PK |
| magazine | VARCHAR | |

#### Table: customer_subscriptions_variants

| Column Name | Data Type | Key |
| --- | --- | --- |
| _**customer2__id**_ | VARCHAR | PK/FK|
| subscriptions_index_lvl_0 | BIGINT | PK/FK |
| subscriptions_variants_index_lvl_0 | BIGINT | PK |
| value | VARCHAR | |

For the tables `customer_address` and `customer_subscriptions` we only need `customer__id`. For `customer_subscriptions_variants`, we need
`customer__id` and `subscriptions_index_lvl_0`. Between these tables, the following join conditions would be allowed while any others would be rejected.

- `SELECT * FROM "customer" LEFT JOIN "customer_subscriptions" ON "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer" LEFT JOIN "customer_address" ON "customer"."customer__id" = "customer_address.customer__id"`
- `SELECT * FROM "customer_address" LEFT JOIN "customer_subscriptions" ON "customer_address"."customer__id" = "customer_subscriptions".customer__id"`
- `SELECT * FROM "customer_subscriptions" LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "customer_subscriptions_variants".customer__id"
  AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants.subscriptions_index_lvl_0"`

These can be combined as long as the complete set of foreign keys are still present.

- ```
  SELECT * FROM "customer_address" LEFT JOIN "customer_subscriptions" ON "customer_address"."customer__id" = "customer_subscriptions".customer__id"
    LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "Customer_subscriptions_variants".customer__id"
    AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants.subscriptions_index_lvl_0"```
- ```
  SELECT * FROM "customer" LEFT JOIN "customer_subscriptions" ON "customers"."customer__id" = "customer_subscriptions".customer__id"
    LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "customer_subscriptions_variants".customer__id"
    AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants"."subscriptions_index_lvl_0"```

This feature allows `INNER` and `LEFT (OUTER)` joins. 
The `NATURAL` and `CROSS` keywords, as well as specifying multiple tables in the `FROM` clause, 
are also accepted as long as the join is still semantically a same collection join on the complete set of foreign keys. 

The following are all equivalent to the same inner join: 

- `SELECT * FROM "customer" INNER JOIN "customer_subscriptions" ON "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer" NATURAL JOIN "customer_subscriptions"` 
- `SELECT * FROM "customer" CROSS JOIN "customer_subscriptions" WHERE "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer", "customer_subscriptions" WHERE "customer"."customer__id" = "customer_subscriptions.customer__id"`

`FULL OUTER` and `RIGHT (OUTER)` joins even with the above join conditions are not supported. 

### Data Types  

-> make a table here lol
Note that while all the types above are supported when constructing queries, columns themselves will only be of type:

### Sorting
When using an `ORDER BY` clause, values corresponding directly to a column will 
be sorted with the underlying DocumentDB type. 
For columns where the corresponding fields in DocumentDB may be of varying types (ex: some are string, null, or integer),
the sort will consider both value and type, following the MongoDB comparison order of types as documented [here]().
This can produce results that are unexpected for those unfamiliar with the underlying data.

### Type Conversion
Type conversions is currently handled as a last step of the query execution.
When used outside the `SELECT` clause or even in a more complex or nested expression in the `SELECT` clause, 
`CAST` may have inconsistent results. 

#### Supported Conversions

### Operators and Functions 

#### Comparison Operators  

##### Supported
| Operator                                          | Description
|:------------------------------------------------- |:-----------
| value1 = value2                                   | Whether *value1* equals *value2*
| value1 <> value2                                  | Whether *value1* is not equal to *value2*
| value1 > value2                                   | Whether *value1* is greater than *value2*
| value1 >= value2                                  | Whether *value1* is greater than *value2*
| value1 < value2                                   | Whether *value1* is less than *value2*
| value1 <= value2                                  | Whether *value1* is less than or equal to *value2*
| value IS NULL                                     | Whether *value* is null
| value IS NOT NULL                                 | Whether *value* is not null
| value1 BETWEEN value2 AND value3                  | Whether *value1* is greater than or equal to *value2* and less than or equal to *value3*
| value1 NOT BETWEEN value2 AND value3              | Whether *value1* is less than *value2* or greater than *value3*
| value1 IN (value2 [, valueN]*)                    | Whether *value1* is equal to a value in the list
| value1 NOT IN (value2 [, valueN]*)                | Whether *value1* is not equal to every value in the list

In this context, values can be a reference to another column, a literal, or some expression consisting of other supported operators.
Values cannot be subqueries. Subqueries are separate queries that can be used as an expression in another query. 

Note also that values that are references to a column are compared with their native DocumentDB type. 
For columns where the corresponding fields in DocumentDB may be of varying types (ex: some are string, null, or integer), 
comparison operators will compare both value and type, following the MongoDB comparison order of types as documented [here]().
This can produce results that are unexpected for those unfamiliar with the underlying data.

##### Unsupported
- value1 IS [NOT] DISTINCT FROM value2
- string1 [NOT] LIKE string2
- string1 [NOT] SIMILAR TO string2
- value [NOT] IN (subquery)
- value <comparison operator> SOME (subquery)
- value <comparison operator> ANY (subquery)
- value <comparison operator> ALL (subquery)
- EXISTS (subquery)

#### Logical Operators
##### Supported
| Operator               | Description
|:---------------------- |:-----------
| boolean1 OR boolean2   | Whether *boolean1* is TRUE or *boolean2* is TRUE
| boolean1 AND boolean2  | Whether *boolean1* and *boolean2* are both TRUE
| NOT boolean            | Whether *boolean* is not TRUE; returns UNKNOWN if *boolean* is UNKNOWN

##### Unsupported 
- boolean IS [NOT] TRUE
- boolean IS [NOT] FALSE
- boolean IS [NOT] UNKNOWN

#### Arithmetic Operators and Functions
##### Supported
| Operator                  | Description
|:------------------------- |:-----------
| numeric1 + numeric2       | Returns *numeric1* plus *numeric2*
| numeric1 - numeric2       | Returns *numeric1* minus *numeric2*
| numeric1 * numeric2       | Returns *numeric1* multiplied by *numeric2*
| numeric1 / numeric2       | Returns *numeric1* divided by *numeric2*
| MOD(numeric1, numeric2)   | Returns *numeric1* modulo *numeric2* AKA *numeric1* % *numeric2*.

##### Unsupported 
- POWER()
- ABS()
- SQRT()
- LN()
- EXP()
- CEIL()
- FLOOR()
- RAND()
- ACOS(), ASIN(), ATAN(), ATAN2(), COS(), COT(), SIN(), TAN()
- DEGREES()
- PI()
- RADIANS()
- ROUND()
- TRUNCATE()
#### String Operators and Functions 
##### Supported
| Operator                                   | Description
|:-------------------------------------------|:-----------
| SUBSTRING(string FROM integer)             | Returns a substring of a character string starting at a given point. Can also use SUBSTRING(string, integer).
| SUBSTRING(string FROM integer FOR integer) | Returns a substring of a character string starting at a given point with a given length. Can also use SUBSTRING(string, integer, integer).

##### Unsupported
- string || string or string + string concatenation
- CHAR_LENGTH()
- UPPER()
- LOWER()
- POSITION()
- TRIM(), LTRIM(), RTRIM()
- OVERLAY() 
- INITCAP()

#### Date/time Functions 
##### Supported
| Operator syntax                                 | Description
|:------------------------------------------------|:-----------
| CURRENT_TIME                                    | Returns the current time in the session time zone.
| CURRENT_DATE                                    | Returns the current date in the session time zone.
| CURRENT_TIMESTAMP                               | Returns the current date and time in the session time zone.
| EXTRACT(timeUnit FROM timestamp)                | Extracts and returns a *timeUnit* part from a *timestamp*.
| FLOOR(timestamp TO timeUnit)                    | Rounds *timestamp* down to *timeUnit*
| YEAR(date)                                      | Returns an integer.
| QUARTER(date)                                   | Returns an integer between 1 and 4.
| MONTH(date)                                     | Returns an integer between 1 and 12.
| WEEK(date)                                      | Returns an integer between 0 and 53. Week starts on Sunday.
| DAYOFYEAR(date)                                 | Returns an integer between 1 and 366.
| DAYOFMONTH(date)                                | Returns an integer between 1 and 31.
| DAYOFWEEK(date)                                 | Returns an integer between 1 and 7. Week starts on Sunday.
| HOUR(date)                                      | Returns an integer between 0 and 23.
| MINUTE(date)                                    | Returns an integer between 0 and 59.
| SECOND(date)                                    | Returns an integer between 0 and 59.
| DAYNAME(date)                                   | Returns the name of the day in the session's locale.
| MONTHNAME(date)                                 | Returns the name of the month in the session's locale.
| TIMESTAMPADD(timeUnit, integer, timestamp)      | Returns *timestamp* with an interval of (signed) *integer* *timeUnit*s added. 
| TIMESTAMPDIFF(timeUnit, timestamp1, timestamp2) | Returns the (signed) number of *timeUnit* intervals between *timestamp1* and *timestamp2*.

`timeUnit` can be one of `YEAR` | `QUARTER` | `MONTH` | `WEEK` | `DOY` | `DOW` | `DAY` | `HOUR` | `MINUTE` 
| `SECOND` | `MICROSECOND` .  

Note that `EXTRACT` for `WEEK` or `WEEK()` return an integer between 0 and 53. This is in line with the MongoDB
interpretation of week numbers but other databases more commonly return a value between 1 and 53.

##### Unsupported 
Addition using the `YEAR`, `QUARTER`, and `MONTH` intervals is not supported in `TIMESTAMPADD`.

#### Conditional Functions and Operators 
##### Supported
| Operator                          | Description
|:----------------------------------|:-----------
| CASE                              | Typical SQL CASE statement. Can use simple or searched case format.
| COALESCE(value, value[, value ]*) | Return the first non-null value in a list.

##### Unsupported 
- NULLIF()

#### Aggregate Functions
##### Supported 
| Operator syntax                               | Description
|:--------------------------------------------- |:-----------
| AVG( [ ALL &#124; DISTINCT ] numeric)         | Returns the average of *numeric* across all values
| COUNT(*)                                      | Returns the number of input rows
| COUNT( [ ALL &#124; DISTINCT ] value)         | Returns the number of input rows for which *value* is not null 
| MAX(value)                                    | Returns the maximum value of *value* across all input values
| MIN(value)                                    | Returns the minimum value of *value* across all input values
| SUM( [ ALL &#124; DISTINCT ] numeric)         | Returns the sum of *numeric* across all input values

For `AVG()` and `SUM()`, if the underlying field in DocumentDb has both numeric and non-numeric values, 
the non-numeric values passed are ignored in the calculation. Note that the sum of a column with all null or unknown values is 0
where in other databases this case would return `null`.

##### Unsupported 
- COUNT([ALL | DISTINCT] value, value[, value]*) 
- STDEV_POP() 
- STDEV_SAMP()
- VAR_POP() 
- VAR_SAMP()