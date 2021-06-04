# Managing Schema Using the Command Line Interface

## Syntax

```
java -j documentdb-jdbc-<version>.jar -g | -r
            -s <host-name> -d <database-name> -u <user-name> [-p <password>] [-t] [-a]
            [-n <schema-name>] [-m <method>] [-l <max-documents>]
            [-h] [--version]
```

### Command Options

The command options specify which function the interface should perform. Exactly one of the command 
options must be provided.

|Option|Description|
|---:|---|
| <span style="white-space: nowrap;">`-g`, <br>`--generate-new`</span> | Generates a new schema for the database. This will have the effect of replacing an existing schema of the same name, if it exists. |
| <span style="white-space: nowrap;">`-r`, <br>`--remove`</span> | Removes the schema from storage for schema given by `-m <schema-name>`, or for schema `_default`, if not provided. |

### Connection Options

The connection options provide the settings needed to connect to your Amazon DocumentDB cluster.

|Option|Description|Default|
|---:|---|---|
| <span style="white-space: nowrap;">`-s`, <br>`--server <host-name>`</span> | The hostname and optional port number (default: `27017`) in the format `hostname[:port]`. (required) | |
| <span style="white-space: nowrap;">`-d`, <br>`--database <database-name>`</span> | The name of the database for the schema operations. (required) | |
| <span style="white-space: nowrap;">`-u`, <br>`--user <user-name>`</span> | The name of the user performing the schema operations. **Note**: *the user will require **readWrite** role on the `<database-name>` where the schema are stored if creating or modifying schema.* (required) | |
| <span style="white-space: nowrap;">`-p`, <br>`--password <password>`</span> | The password for the user performing the schema operations. If this option is not provided, the end-user will be prompted to enter the password directly on the command line. (optional) | |
| <span style="white-space: nowrap;">`-t`, <br>`--tls`</span> | The indicator of whether to use TLS encryption when connecting to DocumentDB. (optional) | `false` |
| <span style="white-space: nowrap;">`-a`, <br>`--tls-allow-invalid-hostnames` </span> | The indicator of whether to allow invalid hostnames when connecting to DocumentDB. (optional) | `false` |

### Schema Options

The schema options provide the setting to override default behavior for schema management.

|Option|Description|Default|
|---:|---|---|
| <span style="white-space: nowrap;">`-n`, <br>`--schema-name <schema-name>`</span> | The name of the schema. (optional) | `_default` |
| <span style="white-space: nowrap;">`-m`, <br>`--scan-method <method>`</span> | The scan method to sample documents from the collections. One of: `random`, `idForward`, `idReverse`, or `all`. Used in conjunction with the `--generate-new` command. (optional) | `random` |
| <span style="white-space: nowrap;">`-l`, <br>`--scan-limit <max-documents>`</span> | The maximum number of documents to sample in each collection. Used in conjunction with the --generate-new command. (optional) | `1000` |

### Miscellaneous Options

The miscellaneous options provide more information about this interface.

|Option|Description|
|---:|---|
| <span style="white-space: nowrap;">`-h`, <br>`--help`</span> | Prints the command line syntax. (optional) |
| <span style="white-space: nowrap;">`--version`</span> | Prints the version number of the command. (optional) |

## Examples

### Default Schema Name

```
> java -jar document-db-1.0.SNAPSHOT-all.jar -g -s localhost:27019 -d test -u ajones -t -a
Password:

New schema '_default', version '1' generated.
```

### Custom Schema Name

```
> java -jar document-db-1.0.SNAPSHOT-all.jar -g -s localhost:27019 -d test -u ajones -t -a -m products
Password:

New schema 'products', version '1' generated.
```

### Removing Custom Schema
```
> java -jar document-db-1.0.SNAPSHOT-all.jar -r -s localhost:27019 -d test -u ajones -t -a -m products
Password:

Removed schema 'products'.
```

### Password as Option

```
> java -jar document-db-1.0.SNAPSHOT-all.jar -g -s localhost:27019 -d test -u ajones -p secret -t -a

New schema '_default', version '2' generated.
```
