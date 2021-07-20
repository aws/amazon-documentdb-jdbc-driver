# JMeter Tests

## Set-Up

### Install & Set Up JMeter   

- Download [JMeter](http://jmeter.apache.org/download_jmeter.cgi).

- Add the [MySQL JDBC auth plugin](https://docs.mongodb.com/bi-connector/current/reference/auth-plugin-jdbc/) 
  to the JMeter `/lib` folder.

- Add a [MySQL driver](https://dev.mysql.com/downloads/connector/j/) 
  `.jar` to the JMeter `/lib` folder. 

- Add the latest DocumentDbDriver `.jar` to the JMeter `/lib` folder.

### Set Up a Test Cluster

If targeting a cluster other than 
`performance-cluster.cluster-c9voe85gof13.ca-central-1.docdb.amazonaws.com`,  
you will need to insert the relevant test data beforehand. 
Use `mongoimport` to insert the data from `/testData`, using the name of each file as the collection name and “jmeter” as the database.

A potential enhancement could be to automate this. 

## Run Tests

### Run Test Plan with DocumentDb Driver

1. Open the `DocumentDb_Test_Plan.jmx` file in JMeter.

1. Start an SSH tunnel for the target cluster.

1. Click on the `DocumentDb Test Plan element`. 
    May need to change user variables such as `CONNECTION_STRING`, `USERNAME`, and `PASSWORD` depending on SSH tunnel setup and target cluster.

1. Run the test plan.

### Run Test Plan with Mongo BI Connector

1. Open the `DocumentDb_Test_Plan.jmx` file in JMeter. 
   Click on the DocumentDb Test Plan element and change the following user variables. 
    
    - Change `QUERY_FORMAT` from `doc_db` to `mongo_bi`.
    - Change `CONNECTION_STRING` to MySQL format and include the JDBC authentication plugin. 
      May be something like: 
      `jdbc:mysql://127.0.0.1:3307?useSSL=false&authenticationPlugins=org.mongodb.mongosql.auth.plugin.MongoSqlAuthenticationPlugin`

1. Start the Mongo BI Connector and wait enough time for schema to be discovered.  
   May be something like: 
   `mongosqld --mongo-uri 'mongodb://127.0.0.1:27017/?connect=direct' -u documentdb -p bqdocumentdblab --mongo-ssl --mongo-sslAllowInvalidHostnames --mongo-sslCAFile rds-combined-ca-bundle.pem --auth`

1. Run test plan.

## Add Tests

### Add Test Data 

- Add test data as a `JSON` file under the `/testData` folder. 
  Use [MongoDB Extended JSON syntax](https://docs.mongodb.com/manual/reference/mongodb-extended-json/) 
  to explicitly specify types. 

### Add Test Cases

- To add a query, you need to add a new row in `Test_Plan.csv`. Because the Mongo BI Connector and  
  the DocumentDB driver use slightly different syntax, there are separate columns for each but 
  the expected result is the same for both formats.  
  
- To keep results uniform between the Mongo BI connector and DocumentDB driver:

    - Columns are ordered explicitly instead of using `SELECT *`  so column order is deterministic.

    - Rows are ordered explicitly with `ORDER BY` so row order is deterministic.

    - Calculated fields are named explicitly with `AS` so they always use same name.