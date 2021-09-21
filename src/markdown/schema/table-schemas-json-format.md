# Table Schemas JSON Format

## Format Description

The exported JSON format has the following characteristics:

1. A non-empty array (list) of table schema.
1. Each table schema is an object with the following fields:
    1. `sqlName` - (_required_) The SQL name of the table used in SQL queries.
       Values are _case-sensitive_.
    1. `collectionName` - (_required_) The name of the DocumentDB collection associated with this table.
       Values are _case-sensitive_.
    1. `columns` - (_required_) The ordered non-empty array (list) of columns in the SQL table.
1. Each element of the `columns` array is and object with the following fields:
    1. `fieldPath` - (_required_) The relative path of the field in the collection document.
       Values are _case-sensitive_.
   1. `sqlName` - (_required_) The SQL name of the column used in SQL queries.
      Values are _case-sensitive_.
    1. `sqlType` - (_required_) The SQL data type for this column in the table.
       Allowed values: `"bigint"`, `"boolean"`, `"decimal"`, `"double"`, `"integer"`, `"null"`, `"timestamp"`, 
       `"varbinary"`, `"varchar"`.
    1. `dbType` - (_required_) The DocumentDB data type from the collection. 
       Allowed values: `"binary"`,`"boolean"`,`"date_time"`,`"decimal128"`,`"double"`,`"int32"`,`"int64"`,
      `"max_key"`,`"min_key"`,`"null"`,`"object_id"`,`"string"`. 
       At this time, the query engine ignores this value. It is recommended to not change this value
       from the generated value.
    1. `isIndex` - (optional) The indicator of whether the column is an index for the underlying array. 
       Allowed values: true or false.   
    1. `isPrimaryKey` - (_optional_) A boolean indicator of whether the column is part of the 
       primary key for this table. Allowed values: `true` or `false`.
    1. `foreignKeyTableName` - (_optional_) The SQL table name of the foreign key.
       Values are _case-sensitive_.
    1. `foreignKeyColumnName` - (_optional_) The SQL column name of the foreign key.
       Values are _case-sensitive_.
    

Although you can add or remove any column as primary or foreign key,
it is not recommended and can generate unexpected behaviour. DocumentDb driver uses
primary and foreign keys to validate and perform table joins from the same collection.

### Uniqueness Requirement

1. Within the list of table objects, the `sqlName` field _should_ be unique. Otherwise, the last 
   table object with that non-unique `sqlName` will replace earlier tables with the same `sqlName` value.
1. Within the list of column object, the `sqlName` field _must_ be unique. Otherwise, the schema will
   fail to load indicating a "duplicate key" exception.

## JSON Schema Validation

You may want to validate your changes to the file using this [JSON schema](table-schemas.schema.json).
Check your changes using an [online site](https://www.jsonschemavalidator.net/) to determine if your
changes will be accepted when importing the file.

## Common Scenarios for Modifying Table Schema

### Flattening sub-documents into the parent table.

In this scenario, the schema generator creates virtual tables for each sub-document in the 
scanned collection. To query the data in the base table and sub-documents, a user must use a `JOIN`
clause to bring the data back together. By flattening the sub-document fields, the field will appear
to belong to the base table. Care must be taken to ensure that the SQL column names added are 
unique within the table.

#### "Products" Collection

Here is an example collection `"products"`.

```json
[ {
  "_id" : "60830884bd61254fc1547e14",
  "name" : "Phone Service Basic Plan",
  "rating" : 3,
  "limits" : {
    "voice" : {
      "units" : "minutes",
      "n" : 400,
      "over_rate" : 0.05
    }
  }
},
{
  "_id" : "60830884bd61254fc1547e15",
  "name" : "Phone Service Core Plan",
  "rating" : 3,
  "limits" : {
    "voice" : {
      "units" : "minutes",
      "n" : 1000,
      "over_rate" : 0.05
    }
  }
},
{
  "_id" : "60830884bd61254fc1547e16",
  "name" : "Phone Service Family Plan",
  "rating" : 4,
  "limits" : {
    "voice" : {
      "units" : "minutes",
      "n" : 1200,
      "over_rate" : 0.05
    }
  }
} ]
```

#### "Products" Generated Schema

The schema generator produces this table schema for the `"products"` base table.

```json
{
  "sqlName" : "products",
  "collectionName" : "products",
  "columns" : [ {
    "fieldPath" : "_id",
    "sqlName" : "products__id",
    "sqlType" : "varchar",
    "dbType" : "string",
    "isPrimaryKey" : true
  }, {
    "fieldPath" : "name",
    "sqlName" : "name",
    "sqlType" : "varchar",
    "dbType" : "string"
  }, {
    "fieldPath" : "rating",
    "sqlName" : "rating",
    "sqlType" : "double",
    "dbType" : "double"
  } ]
}
```

##### SQL Query

When querying the `"products"` table, it produces the following result.

```roomsql
SELECT * FROM "products"
```

| products__id | name | rating |
| --- | --- | --- |
| "60830884bd61254fc1547e0d" | "AC3 Phone" | 3.8 |
| "60830884bd61254fc1547e0e" | "AC7 Phone" | 4.0 |

#### "Products_limits_voice" Generated Schema

The schema generator produces this table schema for the `"products_limits_voice"` virtual table.

```json
{
  "sqlName" : "products_limits_voice",
  "collectionName" : "products",
  "columns" : [ {
    "fieldPath" : "_id",
    "sqlName" : "products__id",
    "sqlType" : "varchar",
    "dbType" : "string",
    "isPrimaryKey" : true,
    "foreignKeyTableName" : "products",
    "foreignKeyColumnName" : "products__id"
  }, {
    "fieldPath" : "limits.voice.units",
    "sqlName" : "units",
    "sqlType" : "varchar",
    "dbType" : "string"
  }, {
    "fieldPath" : "limits.voice.n",
    "sqlName" : "n",
    "sqlType" : "integer",
    "dbType" : "int32"
  }, {
    "fieldPath" : "limits.voice.over_rate",
    "sqlName" : "over_rate",
    "sqlType" : "double",
    "dbType" : "double"
  } ]
}
```


##### SQL Query

When querying the `"products_limits_voice"` table, it produces the following result.

```roomsql
SELECT * FROM "products_limits_voice"
```

| products__id | units | n | over_rate |
| --- | --- | --- | --- |
| "60830884bd61254fc1547e14" | "minutes" | 400 | 0.05 |
| "60830884bd61254fc1547e15" | "minutes" | 1000 | 0.05 |
| "60830884bd61254fc1547e16" | "minutes" | 1200 | 0.05 |

#### Modified "Products" Table Schema

Adding the three columns (`units`, `n`, and `over_rate`) from the `"products_limits_voice"` 
virtual table to the end of the column list for the "products" table.
We've updated the `sqlName` values (`limits_voice_units`, `limits_voice_n`, 
and `limits_voice_over_rate`) to give more context to what these values represent.

```json
[ {
  "sqlName" : "products",
  "collectionName" : "products",
  "columns" : [ {
    "fieldPath" : "_id",
    "sqlName" : "products__id",
    "sqlType" : "varchar",
    "dbType" : "string",
    "isPrimaryKey" : true
  }, {
    "fieldPath" : "name",
    "sqlName" : "name",
    "sqlType" : "varchar",
    "dbType" : "string"
  }, {
    "fieldPath" : "monthly_price",
    "sqlName" : "monthly_price",
    "sqlType" : "double",
    "dbType" : "double"
  }, {
    "fieldPath" : "rating",
    "sqlName" : "rating",
    "sqlType" : "double",
    "dbType" : "double"
  }, {
    "fieldPath" : "limits.voice.units",
    "sqlName" : "limits_voice_units",
    "sqlType" : "varchar",
    "dbType" : "string"
  }, {
    "fieldPath" : "limits.voice.n",
    "sqlName" : "limits_voice_n",
    "sqlType" : "integer",
    "dbType" : "int32"
  }, {
    "fieldPath" : "limits.voice.over_rate",
    "sqlName" : "limits_voice_over_rate",
    "sqlType" : "double",
    "dbType" : "double"
  } ]
} ]
```

##### SQL Query

Now, when using the updated table schema, querying the  `"products"` table, it produces the following result.

```roomsql
SELECT * FROM "products"
```

| products__id | name | rating | limits_voice_units | limits_voice_n | limits_voice_over_rate |
| ---          | ---  | ---    | ---   | --- | ---       |
| "60830884bd61254fc1547e14" | "Phone Service Basic Plan" | 3.0 | "minutes" | 400 | 0.05 |
| "60830884bd61254fc1547e15" | "Phone Service Core Plan" | 3.0 | "minutes" | 1000 | 0.05 |
| "60830884bd61254fc1547e16" | "Phone Service Family Plan" | 4.0 | "minutes" | 1200 | 0.05 |
