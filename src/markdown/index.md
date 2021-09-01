# Amazon DocumentDB JDBC Driver Documentation

## Overview

The JDBC driver for the Amazon DocumentDB managed document database provides an
SQL-relational interface for developers and BI tool users.

## License

This project is licensed under the Apache-2.0 License.

## Documentation

- Setup
    - [Amazon DocumentDB JDBC Driver Setup](setup/setup.md)
    - [Connection String Syntax and Options](setup/connection-string.md)
- Managing Schema
    - [Schema Discovery and Generation](schema/schema-discovery.md)
    - [Managing Schema Using the Command Line Interface](schema/manage-schema-cli.md)
    - [Table Schemas JSON Format](schema/table-schemas-json-format.md)
- SQL Compatibility
    - [SQL Support and Limitations](sql/sql-limitations.md)
- JDBC Compatibility
    - [JDBC Support and Limitations](jdbc/jdbc-limitations.md)
- Support
    - [Troubleshooting Guide](support/troubleshooting-guide.md)
  
## Getting Started

Follow the [requirements and setup directions](setup/setup.md) to get you environment ready to use the
Amazon DocumentDB JDBC driver. Assuming your Amazon DocumentDB cluster is hosted in a private VPC, 
you'll want to [create an SSH tunnel](setup/setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) to bridge to 
your cluster in the VPC. If you're a Tableau or other BI user, follow the directions on how to 
[setup and use BI tools](setup/setup.md#driver-setup-in-bi-applications) with the driver.

## Setup and Usage

To set up and use the DocumentDB JDBC driver, see [Amazon DocumentDB JDBC Driver Setup](setup/setup.md).

## Connection String Syntax

```
jdbc:documentdb://[<user>[:<password>]@]<hostname>[:<port>]/<database-name>[?<option>=<value>[&<option>=<value>[...]]]
```

For more information about connecting to an Amazon DocumentDB database using this JDBC driver, see
the [connection string documentation](setup/connection-string.md) for more details.
## Schema Discovery

The Amazon DocumentDB JDBC driver can perform automatic schema discovery and generate an SQL to
DocumentDB schema mapping. See the [schema discovery documentation](schema/schema-discovery.md)
for more details of this process.

## Schema Management

The SQL to DocumentDB schema mapping can be managed in the following ways:

- generated
- removed
- listed
- exported
- imported

See the [schema management documentation](schema/manage-schema-cli.md) and
[table schemas JSON format](schema/table-schemas-json-format.md) for further
information.

## SQL and JDBC Limitations

The Amazon DocumentDB JDBC driver has a number of important limitations. See the
[SQL limitations documentation](sql/sql-limitations.md) and 
[JDBC limitations documentation](jdbc/jdbc-limitations.md) for more information.

## Troubleshooting Guide

If you're having an issue using the Amazon DocumentDB JDBC driver, consult the
[Troubleshooting Guide](support/troubleshooting-guide.md) to see if has a solution for
your issue.
