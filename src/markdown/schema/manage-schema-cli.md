# Managing Schema Using the Command Line Interface

## Syntax

```
java -jar documentdb-jdbc-<version>.jar [-g | -r | -l | -b | -e <[table-name[,...]]> | -i <file-name>]
        -s <host-name> -d <database-name> -u <user-name> [-p <password>] [-t] [-a]
        [-n <schema-name>] [-m <method>] [-x <max-documents>] [-o <file-name>]
        [-h] [--version]
```

### Command Options

The command options specify which function the interface should perform. Exactly one of the command 
options must be provided.

|Option|Description|
|---:|---|
| `-g`, <br><span style="white-space: nowrap;">`--generate-new`</span> | Generates a new schema for the database. This will have the effect of replacing an existing schema of the same name, if it exists. |
| `-e`, <br><span style="white-space: nowrap;">`--export <[table-name[,...]]>`</span> | Exports the schema to for SQL tables named `[<table-name>[,<table-name>[…]]]`. If no `<table-name>` are given, all table schema will be exported. By default, the schema is written to `stdout`. Use the `-o` option to write to a file. The output format is JSON. |
| `-i`, <br><span style="white-space: nowrap;">`--import <file-name>`</span> | Imports the schema from `<file-name>` in your home directory. The schema will be imported using the `<schema-name>` and a new version will be added - replacing the existing schema. The expected input format is JSON. |
| `-l`, <br><span style="white-space: nowrap;">`--list-schema`</span> | Lists the schema names, version and table names available in the schema repository." |
| `-b`, <br><span style="white-space: nowrap;">`--list-tables`</span> | Lists the SQL table names in a schema." |
| `-r`, <br><span style="white-space: nowrap;">`--remove`</span> | Removes the schema from storage for schema given by `-m <schema-name>`, or for schema `_default`, if not provided. |

### Connection Options

The connection options provide the settings needed to connect to your Amazon DocumentDB cluster.

|Option|Description|Default|
|---:|---|---|
| `-s`, <br><span style="white-space: nowrap;">`--server <host-name>`</span> | The hostname and optional port number (default: `27017`) in the format `hostname[:port]`. (required) | |
| `-d`, <br><span style="white-space: nowrap;">`--database <database-name>`</span> | The name of the database for the schema operations. (required) | |
| `-u`, <br><span style="white-space: nowrap;">`--user <user-name>`</span> | The name of the user performing the schema operations. **Note**: *the user will require **readWrite** role on the `<database-name>` where the schema are stored if creating or modifying schema.* (required) | |
| `-p`, <br><span style="white-space: nowrap;">`--password <password>`</span> | The password for the user performing the schema operations. If this option is not provided, the end-user will be prompted to enter the password directly on the command line. (optional) | |
| `-t`, <br><span style="white-space: nowrap;">`--tls`</span> | The indicator of whether to use TLS encryption when connecting to DocumentDB. (optional) | `false` |
| `-a`, <br><span style="white-space: nowrap;">`--tls-allow-invalid-hostnames` </span> | The indicator of whether to allow invalid hostnames when connecting to DocumentDB. (optional) | `false` |

### Schema Options

The schema options provide the setting to override default behavior for schema management.

|Option|Description|Default|
|---:|---|---|
| `-n`, <br><span style="white-space: nowrap;">`--schema-name <schema-name>`</span> | The name of the schema. (optional) | `_default` |
| `-m`, <br><span style="white-space: nowrap;">`--scan-method <method>`</span> | The scan method to sample documents from the collections. One of: `random`, `idForward`, `idReverse`, or `all`. Used in conjunction with the `--generate-new` command. (optional) | `random` |
| `-x`, <br><span style="white-space: nowrap;">`--scan-limit <max-documents>`</span> | The maximum number of documents to sample in each collection. Used in conjunction with the --generate-new command. (optional) | `1000` |
| `-o`, <br><span style="white-space: nowrap;">`--output <file-name>`</span> | Write the exported schema to `<file-name>` in your home directory (instead of stdout). This will overwrite any existing file with the same name | `stdout` |

### Miscellaneous Options

The miscellaneous options provide more information about this interface.

|Option|Description|
|---:|---|
| `-h`, <br><span style="white-space: nowrap;">`--help`</span> | Prints the command line syntax. (optional) |
| <span style="white-space: nowrap;">`--version`</span> | Prints the version number of the command. (optional) |

## Examples

### Generate Schema using Default Schema Name

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --generate-new \
        --server localhost:27019 --database test -u ajones --tls --tls-allow-invalid-hostnames
Password:

New schema '_default', version '1' generated.
```

### Generate Schema using Custom Schema Name

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --generate-new --schema-name=products \
        --server localhost:27019 --database test -u ajones --tls --tls-allow-invalid-hostnames
Password:

New schema 'products', version '1' generated.
```

### Removing Custom Schema
```
> java -jar document-db-1.0.SNAPSHOT-all.jar --remove --schema-name=products \
        --server localhost:27019 --database test -u ajones --tls --tls-allow-invalid-hostnames
Password:

Removed schema 'products'.
```

### Password as Option

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --generate-new \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames

New schema '_default', version '2' generated.
```

### Listing Schema

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --list-schema \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames

Name=_default, Version=1, SQL Name=test, Modified=2021-06-01T10:35:08-07:00
```

### Listing Table Schema

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --list-tables \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames

products
products_additional_tarriffs
products_for
products_limits
products_limits_data
products_limits_sms
products_limits_voice
products_type
projects
```

### Exporting Schema to Stdout

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --export=products,products_for \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames

[ {
  sqlName : products,
  collectionName : products,
  columns : [ {
    fieldPath : _id,
    sqlName : products__id,
    sqlType : varchar,
    dbType : object_id,
    isPrimaryKey : true
  }, {
    fieldPath : fieldDouble,
    sqlName : fieldDouble,
    sqlType : double,
    dbType : double
  }, {
  
  ...
  
} ]
```

### Exporting Schema to File

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --export=products,products_for -o "sql-schema.json" \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames
> cd ~
> cat sql-schema.json
[ {
  sqlName : products,
  collectionName : products,
  columns : [ {
    fieldPath : _id,
    sqlName : products__id,
    sqlType : varchar,
    dbType : object_id,
    isPrimaryKey : true
  }, {
    fieldPath : fieldDouble,
    sqlName : fieldDouble,
    sqlType : double,
    dbType : double
  }, {
  
  ...
  
} ]
```

### Importing Schema

```
> java -jar document-db-1.0.SNAPSHOT-all.jar --import=sql-schema.json \
        --server localhost:27019 --database test -u ajones -p secret --tls --tls-allow-invalid-hostnames
```
