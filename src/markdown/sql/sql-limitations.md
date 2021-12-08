# SQL Support and Limitations
The Amazon DocumentDB JDBC driver is a read-only driver that supports a subset of SQL-92 and 
some common extensions. 
This section highlights limitations to keep in mind when constructing SQL queries for the driver.

## Basic Query Format
The driver supports `SELECT` statements of the general form: 
```
SELECT [ ALL | DISTINCT ] { * | projectItem [, projectItem ]* }
   FROM tableExpression
   [ WHERE booleanExpression ]
   [ GROUP BY { groupItem [, groupItem ]* } ]
   [ HAVING booleanExpression ]
   [ ORDER BY orderItem [ ASC | DESC ] [, orderItem [ ASC | DESC ]* ]
   [ LIMIT limitNumber ]
   [ OFFSET startNumber ]
```
Queries without a `FROM` clause or only using `VALUES` in the `FROM` clause are not supported.
A `tableExpression` must specify 1 or more tables as a comma separated list or using `JOIN` keywords. See the 
[Joins](#joins) section for more information. All other clauses are optional.

A `projectItem`, `groupItem` or `orderItem` can be a reference to a column, a literal or
some combination of the former using [supported operators or functions](#operators-and-functions). 
A `booleanExpression` is the same but must resolve to a `boolean` value. 

A `projectItem` can be given an alias using `AS`. Items without an explicit name will be given
an auto-generated column label in the result. Ordering using column aliases is allowed.

Currently, any `orderItem` must also be a `projectItem`. To order by a value, it must
be part of the `SELECT` list.

Set operations `UNION`, `INTERSECT` and `EXCEPT` are not supported.
Grouping operations using `CUBE`, `ROLLUP` or `GROUPING SETS` are not supported.
Ordering using `NULLS FIRST` and `NULLS LAST` or by referencing column ordinals is not supported.

## Identifiers
Identifiers are the names of tables, columns, and column aliases in an SQL query.  

Quoting is optional but unquoted identifiers must start with a letter 
and can only contain letters, digits, and underscores. 
Quoted identifiers start and end with double quotes. 
They may contain virtually any character. To include a double quote in an identifier, 
use another double quote to escape it. The maximum identifier length, quoted or unquoted, is 128 characters.

Identifier matching is case-sensitive and identifiers that match a reserved SQL keyword must be quoted 
or use fully qualified names.

## Joins

### Cross Collection Joins

Currently, cross collection joins are not supported.

### Same Collection Joins

Currently, the driver only supports `JOINs` across tables from the same collection as long as
the foreign key and primary key relation are present. This is equivalent to presenting the data in its denormalized
form. For such `JOINs`, the minimal set of foreign keys and their respective primary must be used.
This means the join condition must have an equality match for each primary key column shared between 
the tables and no other match conditions.
In a case that TableA has PK1 and TableB PK1/FK1 and PK2, where TableB.FK1 is the foreign key from TableA.PK1. 
The match condition in the join will only need TableA.PK1 = TableB.PK1 and no other match conditions.

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
| _**customer__id**_ | VARCHAR | PK/FK|
| subscriptions_index_lvl_0 | BIGINT | PK/FK |
| subscriptions_variants_index_lvl_0 | BIGINT | PK |
| value | VARCHAR | |

For the tables `customer_address` and `customer_subscriptions` we only need `customer__id`. For `customer_subscriptions_variants`, we need
`customer__id` and possibly `subscriptions_index_lvl_0` depending on the other joined table. 

Examples of minimal foreign keys with primary keys required in a query:

#### Table: Minimal Foreign Keys with Primary Keys required in 2 Join Table
| Table | Joining Table | FK with PK required |
| --- | --- | --- |
| customer | customer_address| customer__id |
| customer | customer_subscriptions | customer__id |
| customer | customer_subscriptions_variants | customer__id |
| customer_address | customer_subscriptions | customer__id |
| customer_address | customer_subscriptions_variants | customer__id |
| customer_subscriptions | customer_subscriptions_variants | customer__id and subscriptions_index_lvl_0 | 

#### Table: Minimal Foreign Keys with Primary Keys required in 3 Join Table
| Table | First Joining Table | FK with PK required for First Join | Second Joining Table | FK with PK required for Second Join |
| --- | --- | --- | --- | --- |
| customer | customer_address| customer__id | customer_subscriptions |  customer__id |
| customer | customer_subscriptions | customer__id | customer_subscriptions_variants | customer__id and subscriptions_index_lvl_0 |

#### Example of Supported Queries
- `SELECT * FROM "customer" LEFT JOIN "customer_subscriptions" ON "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer" LEFT JOIN "customer_address" ON "customer"."customer__id" = "customer_address.customer__id"`
- `SELECT * FROM "customer_address" LEFT JOIN "customer_subscriptions" ON "customer_address"."customer__id" = "customer_subscriptions".customer__id"`
- `SELECT * FROM "customer_subscriptions" LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "customer_subscriptions_variants".customer__id"
  AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants.subscriptions_index_lvl_0"`
  
These can be combined as long as the minimal common foreign keys with primary keys between tables are present.

- ```
  SELECT * FROM "customer_address" LEFT JOIN "customer_subscriptions" ON "customer_address"."customer__id" = "customer_subscriptions".customer__id"
    LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "Customer_subscriptions_variants".customer__id"
    AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants.subscriptions_index_lvl_0"```
- ```
  SELECT * FROM "customer" LEFT JOIN "customer_subscriptions" ON "customers"."customer__id" = "customer_subscriptions".customer__id"
    LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "customer_subscriptions_variants".customer__id"
    AND "customer_subscriptions"."subscriptions_index_lvl_0" = "customer_subscriptions_variants"."subscriptions_index_lvl_0"```

#### Example of Unsupported Query
- `SELECT * FROM "customer_subscriptions" LEFT JOIN "customer_subscriptions_variants" ON "customer_subscriptions"."customer__id" = "customer_subscriptions_variants".customer__id"`

This feature allows `INNER` and `LEFT (OUTER)` joins. 
The `NATURAL` and `CROSS` keywords, as well as specifying multiple tables in the `FROM` clause, 
are also accepted as long as the join is still semantically a same collection using the minimal set of foreign keys with their primary keys. 

The following are all equivalent to the same inner join: 

- `SELECT * FROM "customer" INNER JOIN "customer_subscriptions" ON "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer" NATURAL JOIN "customer_subscriptions"` 
- `SELECT * FROM "customer" CROSS JOIN "customer_subscriptions" WHERE "customer"."customer__id" = "customer_subscriptions.customer__id"`
- `SELECT * FROM "customer", "customer_subscriptions" WHERE "customer"."customer__id" = "customer_subscriptions.customer__id"`

`FULL OUTER` and `RIGHT (OUTER)` joins even with the above join conditions are not supported.  
Be careful with `NATURAL` keyword as it does not always meet the requirements of a supported join 
condition. Specifying conditions explicitly is recommended.

## Data Types
The driver recognizes the following SQL data types:

- `BOOLEAN`
  * Boolean literals must be `TRUE`, `FALSE` or `UNKNOWN`.
- `TINYINT`
- `SMALLINT`
- `INTEGER` or `INT`
- `BIGINT`
- `DECIMAL`
- `REAL` or `FLOAT`
- `DOUBLE`
- `CHAR` and `VARCHAR`
  * String literal must be enclosed in single-quotes.
- `BINARY` and `VARBINARY`
  * Binary string literals must be preceded by `x` and enclosed in single-quotes. Example: `x’45F0AB’`.
- `DATE` 
  * Date literals must be preceded by `DATE` and be enclosed in single-quotes. 
    Example: `DATE ‘1989-07-11’`.
- `TIME` 
  * Time literals must be preceded by `TIME` and be enclosed in single-quotes. 
    Example: `TIME ‘20:08:30’`.
- `TIMESTAMP` 
  * Timestamp literals must be preceded by `TIMESTAMP` and be enclosed in single-quotes. 
    Example: `TIMESTAMP ‘1989-07-11 20:08:30'`.

Note that while all the above can be used when constructing queries, columns themselves can 
only be of the types `BOOLEAN`, `BIGINT`, `INTEGER`, `DECIMAL`, `DOUBLE`, `VARCHAR`, `VARBINARY` and `TIMESTAMP`. 
See the [schema discovery and generation](../schema/schema-discovery.md) sections for more information. 

Note also that double-quotes **cannot** be used in place of single-quotes for specifying literals.

## Sorting
When using an `ORDER BY` clause, values corresponding directly to a column will 
be sorted with the underlying DocumentDB type. 
For columns where the corresponding fields in DocumentDB
may be of varying types (ex: some are `string`, `null`, or `integer`),
the sort will consider both value and type, 
following the [MongoDB comparison order](https://docs.mongodb.com/manual/reference/bson-type-comparison-order/).
This can produce results that are unexpected for those unfamiliar with the underlying data. 

Note that null values will be first when sorting by `ASC` and last when sorting by `DESC` and that
this cannot be overridden since `NULLS FIRST` and `NULLS LAST` are not supported.

## Type Conversion
Type conversions through `CAST`are currently handled as a last step of the query execution.
When used outside the `SELECT` clause or even in a more complex or nested expression in the `SELECT` clause, 
`CAST` may have inconsistent results.

The following conversions using an explicit `CAST` are supported (denoted by **Y**) or unsupported (denoted by **N**):

|  FROM - TO                      | BOOLEAN | TINYINT, SMALLINT, INT, BIGINT | DECIMAL, FLOAT, REAL, DOUBLE | DATE | TIME | TIMESTAMP | CHAR, VARCHAR | BINARY, VARBINARY
|:-------------------             |:--------|:------------------------------ |:-----------------------------|:-----|:-----|:----------|:--------------|:------------------
| BOOLEAN                         | _       | N                              | N                            | N    | N    | N         | Y             | N
| TINYINT, SMALLINT, INT, BIGINT  | N       | Y                              | Y                            | N    | N    | N         | N             | N
| DECIMAL, FLOAT, REAL, DOUBLE    | N       | Y                              | Y                            | N    | N    | N         | N             | N
| DATE                            | N       | N                              | N                            | _    | Y    | Y         | Y             | N
| TIME                            | N       | N                              | N                            | N    | _    | N         | N             | N
| TIMESTAMP                       | N       | N                              | N                            | Y    | Y    | _         | Y             | N
| CHAR, VARCHAR                   | N       | N                              | N                            | N    | N    | N         | _             | N
| BINARY, VARBINARY               | N       | N                              | N                            | N    | N    | N         | N             | _

The driver also allows for implicit conversions when such a conversion makes sense. For example, a `DATE` value passed to 
a function that takes `TIMESTAMP`, will be automatically cast to `TIMESTAMP`.

## Operators and Functions 

### Comparison Operators
- `value1  <op> value2`  where `op` is one of : `=`, `<>`, `<`, `>`, `<=` or `>=`
- `value IS NULL`
- `value IS NOT NULL`
- `value1 BETWEEN value2 AND value3`
- `value1 NOT BETWEEN value2 AND value3`
- `value1 IN (value2 [, valueN]*)`
- `value1 NOT IN (value2 [, valueN]*)`

In this context, `valueN` can be a reference to another column, a literal, 
or some expression consisting of other supported operators.
It **cannot** be a subquery. Subqueries are separate queries that can be used as an expression in another query. 
The driver does not currently support using subqueries as scalar values.

Note also that values that are references to a column are compared with their native DocumentDB type. 
For columns where the corresponding fields in DocumentDB
may be of varying types (ex: some are `string`, `null`, or `integer`), 
comparison operators will compare both value and type,
following the [MongoDB comparison order](https://docs.mongodb.com/manual/reference/bson-type-comparison-order/).
This can produce results that are unexpected for those unfamiliar with the underlying data.

### Logical Operators
- `boolean1 OR boolean2`
- `boolean1 AND boolean2`
- `NOT boolean`

### Arithmetic Operators and Functions
- `numeric1 <op> numeric2` where `op` is one of : `+`, `-`, `*`, or `/`
- `MOD(numeric1, numeric2)`

Note that using `%` as a modulo operator is not supported. Use `MOD` instead.

### String Operators and Functions  
- `string || string` or `{fn CONCAT(string, string)}` to concatenate two strings
- `CONCAT(string [, string ]*)` to concatenate two or more strings
- `CHAR_LENGTH(string)` or `CHARACTER_LENGTH(string)` or `{fn LENGTH(string)}`
- `LEFT(string, length)` or `{fn LEFT(string, length)}` where `length` is an integer
- `LOWER(string)`or `{fn LCASE(string)}` 
- `POSITION(substring IN string)` or `{fn LOCATE(substring, string)}`
- `POSITION(substring IN string FROM offset)` or `{fn LOCATE(substring, string, offset)}` 
  where `offset` is an integer
- `RIGHT(string, length)` or `{fn RIGHT(string, length)}` where `length` is an integer
- `SUBSTRING(string FROM offset)` or `SUBSTRING(string, offset)` or  `{fn SUBSTRING(string, offset)}` where `offset` is an integer
- `SUBSTRING(string FROM offset FOR length)` or `SUBSTRING(string, offset, length)` 
or `{fn SUBSTRING(string, offset, length)}` where `offset` and `length` are integers
- `UPPER(string)` or `{fn UCASE(string)}`

### Date/time Functions
- `CURRENT_TIME`
- `CURRENT_DATE`
- `CURRENT_TIMESTAMP`
- `EXTRACT(timeUnit FROM timestamp)`
- `FLOOR(timestamp TO timeUnit)`
- `YEAR(date)`
- `QUARTER(date)`
- `MONTH(date)`
- `WEEK(date)`
- `DAYOFYEAR(date)`
- `DAYOFMONTH(date)`
- `DAYOFWEEK(date)`
- `HOUR(date)`
- `MINUTE(date)`
- `SECOND(date)`
- `DAYNAME(date)`
- `MONTHNAME(date)`
- `TIMESTAMPADD(timeUnit, integer, timestamp)`
- `TIMESTAMPDIFF(timeUnit, timestamp1, timestamp2)`

In this context, a `date` value can be any value that resolves to a `DATE` or `TIMESTAMP` type. 
Functions that accept a `timestamp` value that are passed a `DATE` type will implicitly convert the value to 
a `TIMESTAMP`, padding the time portion with 0s.

`timeUnit` can be one of `YEAR` | `QUARTER` | `MONTH` | `WEEK` | `DOY` | `DOW` | `DAY` | `HOUR` | `MINUTE` 
| `SECOND` | `MICROSECOND` .

Note that `EXTRACT` for `WEEK` or `WEEK()` return an integer between 0 and 53. This is in line with the MongoDB
interpretation of week numbers but other databases more commonly return a value between 1 and 53.

Addition using the `YEAR`, `QUARTER`, and `MONTH` intervals are not supported in `TIMESTAMPADD`.

### Conditional Functions and Operators
- `CASE` using either the simple or searched `CASE` formats
- `COALESCE(value, value[, value ]*)`

### Aggregate Functions
-  `AVG([ ALL | DISTINCT ] numeric)`
-  `COUNT(*)`
-  `COUNT([ ALL | DISTINCT ] value)`
-  `MAX(value)`
-  `MIN(value)` 
-  `SUM([ ALL | DISTINCT ] numeric)`
   
For `AVG()` and `SUM()`, if the underlying field in DocumentDb has both numeric and non-numeric values, 
the non-numeric values passed are ignored in the calculation. Note that the sum of a column with all null or unknown values is 0
where in other databases this case would return `null`.

`COUNT([ALL | DISTINCT] value, value[, value]*)` is not supported.