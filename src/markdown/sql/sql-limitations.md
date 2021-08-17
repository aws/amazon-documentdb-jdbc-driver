# SQL Support and Limitations

## Basic Query Format
The driver supports `SELECT` statements of the general form: 
```
SELECT [ ALL | DISTINCT ] { * | projectItem [, projectItem ]* }
   FROM tableExpression
   [ WHERE booleanExpression ]
   [ GROUP BY { groupItem [, groupItem ]* } ]
   [ HAVING booleanExpression ]
   [ ORDER BY orderItem [, orderItem ]* ]
   [ LIMIT limitNumber ]
   [ OFFSET startNumber ]
```
Queries without a `FROM` clause or only using `VALUES` in the `FROM` clause are not supported.
A `tableExpression` must specify 1 or more tables as a comma separated list or using `JOIN` keywords. See the 
[Joins](#joins) section for more information. 

All other clauses apart from `SELECT` and `FROM` are optional.

A `projectItem`, `groupItem` or `orderItem` can be a reference to a column, a literal or
some combination of the former using [supported operators or functions](#operators-and-functions). 
A `booleanExpression` is the same but must resolve to a boolean value.

Set operations `UNION`, `INTERSECT` and `EXCEPT` are not supported.
Grouping operations using `CUBE`, `ROLLUP` or `GROUPING SETS` are not supported.
Ordering using `NULLS FIRST` or `NULLS LAST` is not supported.

Note that since it is read-only, the driver does not support any kind of 
`CREATE`, `UPDATE`, `DELETE` or `INSERT` statements. The creation of 
temporary tables using `SELECT INTO` is also not supported.

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

## Data Types
The driver recognizes the following SQL data types:

- `BOOLEAN`
- `TINYINT`
- `SMALLINT`
- `INTEGER` or `INT`
- `BIGINT`
- `DECIMAL`
- `REAL` or `FLOAT`
- `DOUBLE`
- `CHAR`
- `VARCHAR`
- `BINARY`
- `VARBINARY`
- `DATE`
- `TIME`
- `TIMESTAMP`

Note that while all the above can be used when constructing queries, columns themselves can 
only be of the types `BOOLEAN`, `BIGINT`, `INTEGER`, `DECIMAL`, `DOUBLE`, `VARCHAR`, `VARBINARY` and `TIMESTAMP`. 
See the [schema discovery or generation](schema/schema-discovery.md) sections for more information.

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
| DECIMAL, FLOAT, REAL, DOUBLE    |         | Y                              | Y                            | N    | N    | N         | N             | N
| DATE                            | N       | N                              | N                            | _    | Y    | Y         | Y             | N
| TIME                            | N       | N                              | N                            | N    | _    | N         | N             | N
| TIMESTAMP                       | N       | N                              | N                            | Y    | Y    | _         | Y             | N
| CHAR, VARCHAR                   | N       | N                              | N                            | N    | N    | N         | _             | N
| BINARY, VARBINARY               | N       | N                              | N                            | N    | N    | N         | N             | _

The driver also allows for implicit conversions when such a conversion makes sense. For example, a `DATE` value passed to 
a function that takes `TIMESTAMP`, will be automatically cast to `TIMESTAMP`.

## Operators and Functions 

### Comparison Operators
- `value1  < op > value2`  where `op` is one of : `=`, `<>`, `<`, `>`, `<=` or `>=`
- `value IS NULL`
- `value IS NOT NULL`
- `value1 BETWEEN value2 AND value3`
- `value1 NOT BETWEEN value2 AND value3`
- `value1 IN (value2 [, valueN]*)`
- `value1 NOT IN (value2 [, valueN]*)`

In this context, values can be a reference to another column, a literal, 
or some expression consisting of other supported operators.
Values cannot be subqueries. 
Subqueries are separate queries that can be used as an expression in another query. 

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
- `SUBSTRING(string FROM integer)` or `SUBSTRING(string, integer)`
- `SUBSTRING(string FROM integer FOR integer)` or `SUBSTRING(string, integer, integer)`

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

Addition using the `YEAR`, `QUARTER`, and `MONTH` intervals is not supported in `TIMESTAMPADD`.

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