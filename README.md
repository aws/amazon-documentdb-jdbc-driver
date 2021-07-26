# DocumentDB JDBC Driver
![Code Coverage Instructions](./.github/badges/jacoco.svg)
![Code Coverage Branches](./.github/badges/branches.svg)

## Overview

The JDBC driver for the Amazon DocumentDB managed document database provides an 
SQL-relational interface for developers and BI tool users.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

## Documentation

See the [product documentation](src/markdown/index.md) for more detailed information about this driver.

## Setup and Usage

To setup and use the DocumentDB JDBC driver, follow [these directions](src/markdown/setup/setup.md).

## Connection String Syntax

```
jdbc:documentdb://[<user>[:<password>]@]<hostname>[:<port>]/<database-name>[?<option>=<value>[&<option>=<value>[...]]]
```

For more information about connecting to an Amazon DocumentDB database using this JDBC driver, see
the [connection string documentation](src/markdown/setup/connection-string.md) for more details.

## Schema Discovery

The Amazon DocumentDB JDBC driver can perform automatic schema discovery and generate an SQL to 
DocumentDB schema mapping. See the [schema discovery documentation](src/markdown/schema/schema-discovery.md) 
for more details of this process.

## Schema Management

Schema can be managed in the following ways:

- generated
- removed
- listed
- exported
- imported
  
See the [schema management documentation](src/markdown/schema/manage-schema-cli.md) and 
[table schemas JSON format](src/markdown/schema/table-schemas-json-format.md) for further 
information.

## SQL and JDBC Limitations

The Amazon DocumentDB JDBC driver has a number of important limitations. See the 
[SQL and JDBC limitations documentation](src/markdown/sql/sql-jdbc-limitations.md) for more information.

## Troubleshooting Guide

If you're having an issue using the Amazon DocumentDB JDBC driver, consult the 
[Troubleshooting Guide](src/markdown/support/trouble-shooting.md) to see if has a solution for 
your issue.
