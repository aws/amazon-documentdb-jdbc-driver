# DocumentDB JDBC Driver
![Code Coverage Instructions](./.github/badges/jacoco.svg)
![Code Coverage Branches](./.github/badges/branches.svg)

## Overview

The JDBC driver for the Amazon DocumentDB managed document database provides an 
SQL-relational interface for developers and BI tool users.

## Connection String Syntax

`jdbc:documentdb://[<user>[:<password>]@]<hostname>[:<port>]/<database-name>[?<option>=<value>[&<option>=<value>[...]]]`

### Scheme

`jdbc:documentdb:` Required: the scheme for this JDBC driver.

### Parameters

- `<user>` Optional: the username of the authorized user. While the username is optional on the
  connection string, it is still required either via the connection string, or the properties.
  _Note: the username must be properly (%) encoded to avoid any confusion with URI special 
  characters._
- `<password>` Optional: the password of the authorized user. While the password is optional on the 
  connection string, it is still required either via the connection string, or the properties.
  _Note: the password must be properly (%) encoded to avoid any confusion with URI special 
  characters._
- `<hostname>` Required: the hostname or IP address of the DocumentDB server or cluster.
- `<port>` Optional: the port number the DocumentDB server or cluster is listening on.
- `<database>` Required: the name of the database the JDBC driver will connect to.
- `<option>` Optional: one of the connection string options listed below.
- `<value>` Optional: the associated value for the option.

### Options

- `appName` (string) : Sets the logical name of the application.
- `loginTimeoutSec` (int) : How long a connection can take to be opened before timing out (in seconds). 
  Alias for connectTimeoutMS but using seconds.
- `readPreference` (enum/string) : The read preference for this connection. Allowed values: 
  - `primary` (default)
  - `primaryPreferred`
  - `secondary`
  - `secondaryPreferred`
  - `nearest`.
- `replicaSet` (string) : Name of replica set to connect to. For now, passing a name other than 
  rs0 will log a warning.
- `retryReads` (true|false) : If true, the driver will retry supported read operations if they 
  fail due to a network error. Defaults to true.
- `tls` (true|false) : If true, use TLS encryption when communicating with the DocumentDB server.
  Defaults to true.
- `tlsAllowInvalidHostnames` (true|false) : If true, invalid host names for the TLS certificate
  are allowed. This is useful when using an SSH tunnel to a DocumentDB server. Defaults to false.
- `tlsCAFile` (string) : The path to the trusted Certificate Authority (CA) `.pem` file. If the
  path starts with the tilde character (`~`), it will be replaced with the user's home directory.
  Ensure to use only forward slash characters (`/`) in the path or URL encode the path. Providing
  the trusted Certificate Authority (CA) `.pem` file is optional as the current Amazon RDS root CA
  is used by default when the `tls` option is set to `true`. This embedded certificate is set to
  expire on 2024-08-22. For example, to provide a new trusted Certificate Authority (CA) `.pem`
  file that is located in the current user's `Downloads` subdirectory of their home directory,
  use the following: `tlsCAFile=~/Downloads/rds-ca-2019-root.pem`.
- `scanMethod` (enum/string) : The scanning (sampling) method to use when discovering collection 
  metadata for determining table schema. Possible values include the following:
  - `random` - (default) The sample documents are returned in _random_ order.
  - `idForward` - The sample documents are returned in order of id.
  - `idReverse` - The sample documents are returned in reverse order of id.
  - `all` - Sample all the documents in the collection.
- `scanLimit` (int) The number of documents to sample. The value must be a positive integer.
  The default value is `1000`. If `scanMethod` is set to `all`, this option is ignored. 
- `schemaName` (string) The name of the SQL mapping schema for the database. Defaults to `_default`.

## Schema Discovery

### Automated Discovery Behavior

When the JDBC driver connection needs to get the schema for the collection in the database,
it will poll for all the collections in the database.

The driver will determine if a cached version of the schema for that collection already exists. 
If a cached version does not exist, it will sample the collection for documents and create a schema
based on the following behavior.

#### Scanning Method Options

The sampling behavior can be modified using connection string or datasource options.

- `scanMethod=<option>`
  - `random` - (default) The sample documents are returned in _random_ order.
  - `idForward` - The sample documents are returned in order of id.
  - `idReverse` - The sample documents are returned in reverse order of id.
  - `all` - Sample all the documents in the collection.
- `scanLimit=<n>` - The number of documents to sample. The value must be a positive integer.
  The default value is `1000`. If `scanMethod` is set to `all`, this option is ignored.

#### DocumentDB Data Types

The DocumentDB server supports a number of MongoDB data types. Listed below are the supported data
types, and their associated JDBC data types.

| MongoDB Data Type | Supported in DocumentDB | JDBC Data Type |
| ----------- | ----------- | ----------- |
| Binary Data| Yes | VARBINARY |
| Boolean | Yes | BOOLEAN |
| Double | Yes | DOUBLE |
| 32-bit Integer | Yes | INTEGER |
| 64-bit Integer | Yes | BIGINT |
| String | Yes | VARCHAR |
| ObjectId | Yes | VARCHAR |
| Date | Yes | TIMESTAMP |
| Null | Yes | VARCHAR |
| Regular Expression | Yes | VARCHAR |
| Timestamp | Yes | VARCHAR |
| MinKey | Yes | VARCHAR |
| MaxKey | Yes | VARCHAR |
| Object | Yes | _virtual table_ |
| Array | Yes | _virtual table_ |
| Decimal128 | No | DECIMAL |
| JavaScript | No | VARCHAR |
| JavaScript (with scope) | No | VARCHAR |
| Undefined | No | VARCHAR |
| Symbol | No | VARCHAR |
| DBPointer (4.0+) | No | VARCHAR |

#### Mapping Scalar Document Fields

When scanning a sample of documents from a collection, the JDBC driver will create one or more 
schema to represent the samples in the collection. In general, a scalar field in the document 
maps to a column in the table schema. For example, in a collection named `team`, and a single 
document `{ "_id" : "112233", "name" : "Alastair", "age" : 25 }`, this would map to schema:

| Table Name | Column Name | Data Type | Key |
| ---| --- | --- | --- |
| team | _**team__id**_ | VARCHAR | PK |
| team | name | VARCHAR | |
| team | age | INTEGER | |

#### Data Type Conflict Promotion

When scanning the sampled documents, it is possible that the data types for a field are not 
consistent from document to document. In this case, the JDBC driver will _promote_ the JDBC data 
type to a common data type that will suit all data types from the sampled documents.

##### Example

```json
{
  "_id" : "112233",
  "name" : "Alastair",
  "age" : 25
}
```

```json
{
  "_id" : "112244",
  "name" : "Benjamin",
  "age" : "32"
}
```

The `age` field is of type _32-bit integer_ in the first document but _string_ in the second document. 
Here the JDBC driver will promote the JDBC data type to VARCHAR to handle either data type when 
encountered.

| Table Name | Column Name | Data Type | Key |
| ---| --- | --- | --- |
| team | _**team__id**_ | VARCHAR | PK |
| team | name | VARCHAR | |
| team | age | VARCHAR | |

#### Scalar-Scalar Conflict Promotion

The following diagram shows the way in which scalar-scalar data type conflicts are resolved.

![Scalar-Scalar Promotion](images/ScalarDataTypePromotion-transparent.png)

#### Object and Array Data Type Handling

So far, we've only described how scalar data types are mapped. Object and Array data types are 
(currently) mapped to virtual tables. The JDBC driver will create a virtual table to represent 
either object or array fields in a document. The name of the mapped virtual table will concatenate the 
original collection's name followed by the field's name separated by an underscore character ("_").

The base table's primary key ("_id") takes on a new name in the new
virtual table and is provided as a foreign key to the associated base table.

For embedded array type fields, index columns are generated to represent the 
index into the array at each level of the array.

##### Embedded Object Field Example

For object fields in a document, a mapping to a virtual table is created by the
JDBC driver.

Collection: `customer`

```json
{
  "_id" : "112233",
  "name" : "George Jackson",
  "address" : {
    "address1" : "123 Avenue Way",
    "address2" : "Apt. 5",
    "city" : "Hollywood",
    "region" : "California",
    "country" : "USA",
    "code" : "90210"
  }
}
```

maps to schema for `customer` table, ...

| Table Name | Column Name | Data Type | Key |
| --- | --- | --- | --- |
| customer | _**customer__id**_ | VARCHAR | PK |
| customer | name | VARCHAR | |

... and the `customer_address` virtual table

| Table Name | Column Name | Data Type | Key |
| --- | --- | --- | --- |
| customer_address | _**customer__id**_ | VARCHAR | PK/FK |
| customer_address | address1 | VARCHAR | |
| customer_address | address2 | VARCHAR | |
| customer_address | city | VARCHAR | |
| customer_address | region | VARCHAR | |
| customer_address | country | VARCHAR | |
| customer_address | code | VARCHAR | |

So the resulting data in the two tables would look like this...

##### Table: customer

| _**customer__id**_ | name |
| --- | --- |
| "112233" | "George Jackson" |

##### Virtual Table: customer_address

| _**customer__id**_ | address1 | address2 | city | region | country | code |
| --- | --- | --- | --- | --- | --- | --- |
| "112233" | "123 Avenue Way" | "Apt. 5" | "Hollywood" | "California" | "USA" | "90210" |

To query the data and return all columns, use the following query with a
JOIN statement to get the matching address data.

```mysql-sql
SELECT * FROM "customer"
  INNER JOIN "customer_address"
    ON "customer"."customer__id" = "customer_address"."customer__id"
```

##### Embedded Array Field Example

For array fields in a document, a mapping to a virtual table is also created by the
JDBC driver.

Collection: `customer1`

```json
{
  "_id" : "112233",
  "name" : "George Jackson",
  "subscriptions" : [
    "Vogue",
    "People",
    "USA Today"
  ]
}
```

maps to schema for the `customer1` table, ...

| Table Name | Column Name | Data Type | Key |
| --- | --- | --- | --- |
| customer1 | _**customer1__id**_ | VARCHAR | PK |
| customer1 | name | VARCHAR | |

... and the `customer1_subscriptions` virtual table

| Table Name | Column Name | Data Type | Key |
| --- | --- | --- | --- |
| customer1_subscriptions | _**customer1__id**_ | VARCHAR | PK/FK |
| customer1_subscriptions | subscriptions_index_lvl0 | BIGINT | PK |
| customer1_subscriptions | value | VARCHAR | |

So the resulting data in the two tables would look like this...

##### Table: customer1

| _**customer1__id**_ | name |
| --- | --- |
| "112233" | "George Jackson" |

##### Virtual Table: customer1_subscriptions

| _**customer1__id**_ | subscriptions_index_lvl0 | value |
| --- | --- | --- |
| "112233" | 0 | "Vogue" |
| "112233" | 1 | "People" |
| "112233" | 2 | "USA Today" |

To query the data and return all columns, use the following query with a
JOIN statement to get the matching _subscriptions_ data.

```mysql-sql
SELECT * FROM "customer1"
  INNER JOIN "customer1_subscriptions"
    ON "customer"."customer1__id" = "customer_address"."customer1__id"
```

#### Scalar-Complex Type Conflict Promotion

Like the scalar-scalar type conflicts, the same field in different documents can have conflicting
data types between complex (array and object) and scalar (integer, boolean, etc.). All of these
conflicts are resolved (promoted) to VARCHAR for those fields. In this case, array and object data
is returned as the JSON representation.

##### Embedded Array - String Field Conflict Example

Collection: `customer2`

```json
{
  "_id" : "112233",
  "name" : "George Jackson",
  "subscriptions" : [
    "Vogue",
    "People",
    "USA Today"
  ]
}
```

```json
{
  "_id" : "112244",
  "name" : "Joan Starr",
  "subscriptions" : 1
}
```

maps to schema for the `customer2` table, ...

| Table Name | Column Name | Data Type | Key |
| --- | --- | --- | --- |
| customer2 | _**customer2__id**_ | VARCHAR | PK |
| customer2 | name | VARCHAR | |
| customer2 | subscription | VARCHAR | |

So the resulting data in the table would look like this...

##### Table: customer2

| _**customer2__id**_ | name | subscriptions |
| --- | --- | --- |
| "112233" | "George Jackson" | "\[ \\"Vogue\\", \\"People\\",  \\"USA Today\\" \]" |
| "112244" | "Joan Starr" | "1" |


## ResultSet Limitations

  Every `ResultSet` returned by the driver will have a read-only concurrency mode, 
  a forward fetch direction, and a forward-only cursor. 
  As such, this limits the methods available on a `ResultSet`.
  Of the JDBC API's `ResultSet` [methods](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html), 
  the following are unsupported or supported with some limitations:

  ### Unsupported methods 
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

## Join Limitations 

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
| subscriptions_index_lvl_0 | BIGINT | PK/FK |
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

This feature allows `INNER` and `LEFT (OUTER) JOINs` .

# Setup and Usage

To setup and use the DocumentDB JDBC driver, follow [these directions](src/markdown/setup/setup.md).