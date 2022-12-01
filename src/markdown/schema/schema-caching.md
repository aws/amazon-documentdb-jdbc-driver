# Schema Caching

## Schema Caching Behaviour

When a connection is made to an Amazon DocumentDB database, the Amazon DocumentDB JDBC driver 
checks for a previously cached version of the mapped schema. If a previous version exists, 
the latest version of the cached schema is read and used for all further interaction with the database.

If a previously cached version does not exist, the process of [schema discover](schema-discovery.md) is automatically
started on all the accessible collections in the database. The discovery process uses the properties 
`scanMethod` (default `random`), and `scanLimit` (default `1000`) when sampling documents from the database.
At the end of the discovery process, the resulting schema mapping is written to the cache using the name
associated with the property `schemaName` (default `_default`).

If some reason the resulting schema cannot be saved to the cache, the resulting schema will still be used
in-memory for the life of the connection. The implication of not having access to a cached version of the
schema is that the schema discovery will have to be performed for each connection - which could have a seriously 
negative impact on performance.

## Cache Location

The SQL schema mapping cache is stored in two collections on the same database as
the sampled collections. The collection `_sqlSchemas` stores the names and versions of
all the sampled schemas for the given database. The collection `_sqlTableSchemas` stores the 
column to field mappings for all the cached SQL schema mappings. The two cache collections
have a strong parent/child relationship and must be maintained in a consistent way. Always use
the [schema management CLI](manage-schema-cli.md) to ensure consistency in the cache collections.

## User Permissions for Creating and Updating the Schema Cache

To be able to store or update the SQL schema mappings to the cache collections, the connected
Amazon DocumentDB user account must have write permissions to create and update the
cache collections. Once the schema is cached, users need only read permission on the
cache collections.

To allow access for an Amazon DocumentDB user, ensure to set or add the appropriate roles as 
described below.

### Enable Access per Database

To allow read and write access to specific databases in your server, add 
a `readWrite` [built-in role](https://www.mongodb.com/docs/manual/reference/built-in-roles/#mongodb-authrole-readWrite) 
for each database the user should have access to be able to create and update the cached schema for specific 
databases.

```json
roles: [
  {role: "readWrite", db: "yourDatabase1"},
  {role: "readWrite", db: "yourDatabase2"} ...
]
```

### Enable Access for Any Database

To allow read and write access to any databases in your server, add
a `readWriteAnyDatabase` [built-in role](https://www.mongodb.com/docs/manual/reference/built-in-roles/#mongodb-authrole-readWriteAnyDatabase)
on the `admin` database to be able to create and update the cached schema in any database.

```json
roles: [
  {role: "readWriteAnyDatabase", db: "admin"}
]
```

### Collection-Level Access Control

If [collection-level access control](https://www.mongodb.com/docs/manual/core/collection-level-access-control/)
is implemented, then ensure `find`, `insert`, and `update` actions are 
allowed on the cache collections (`_sqlSchemas` and `_sqlTableSchemas`)

## User Permissions for Reading an Existing Schema Cache

To be able to read the SQL schema mappings to the cache collections, the connected
Amazon DocumentDB user account must have read permissions to read the
cache collections. 

To allow access for an Amazon DocumentDB user, ensure to set or add the appropriate roles as
described below.

### Enable Access per Database

To allow read access to specific databases in your server, add
a `read` [built-in role](https://www.mongodb.com/docs/manual/reference/built-in-roles/#mongodb-authrole-read)
for each database the user should have access to be able to read the cached schema for specific
databases.

```json
roles: [
  {role: "read", db: "yourDatabase1"},
  {role: "read", db: "yourDatabase2"} ...
]
```

### Enable Access for Any Database

To allow read access to any databases in your server, add
a `readAnyDatabase` [built-in role](https://www.mongodb.com/docs/manual/reference/built-in-roles/#mongodb-authrole-readAnyDatabase)
on the `admin` database to be able to read the cached schema in any database.

```json
roles: [
  {role: "readAnyDatabase", db: "admin"}
]
```

### Collection-Level Access Control

If [collection-level access control](https://www.mongodb.com/docs/manual/core/collection-level-access-control/)
is implemented, then ensure `find` actions are
allowed on the cache collections (`_sqlSchemas` and `_sqlTableSchemas`)

