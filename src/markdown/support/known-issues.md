# Known Issues and Planned Future Enhancements

- LIMITATION: Schema scan method 'RANDOM' is not supported on DocumentDB Elastic Cluster
  - Please choose another scan method when connecting to a DocumentDB Elastic Cluster
- BUG: The use of `CASE WHEN` for non-numeric types may throw an exception.
- BUG: ResultMetaData reports `VARCHAR` on a column that would normally report `NULL`.
- BUG: Some JavaDocs are incomplete for some interfaces.
- BUG: Using a literal comparison that resolves to `FALSE` throws exception (e.g., `WHERE 3 > 4`)
- BUG: Method `Driver.getParentLogger` is not implemented.
- ENHANCEMENT: URL-encode user-name and password in Tableau Connector
- ENHANCEMENT: Methods `DatabaseMetaData.getIndexInfo` and `DatabaseMetaData.getBestRowIdentifier` are not implemented.
- ENHANCEMENT: Implement a fully functional PreparedStatement with replaceable query parameters.
- ENHANCEMENT: Implement SQL hints for supporting 'allowDiskUse' on a query-by-query basis.
