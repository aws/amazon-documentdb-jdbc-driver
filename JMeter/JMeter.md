# JMeter Tests

## Set-Up

### Install & Set Up JMeter   

- Download [JMeter](http://jmeter.apache.org/download_jmeter.cgi) (should be >= version 5.4.1).

- Add the latest DocumentDbDriver `.jar` to `/lib` folder of your JMeter installation.

### Set Up a Test Cluster

If targeting a cluster other that has not been used for JMeter testing before,  
you will need to insert the relevant test data beforehand. 
Use `mongoimport` to insert the data from `/testData`, using the name of each file as the collection name and “jmeter” as the database.

A potential enhancement could be to automate this. 

## Run Tests

### Run Test Plan with DocumentDb Driver

1. Open the `DocumentDb_Test_Plan.jmx` file in JMeter.

1. Start an SSH tunnel for the target cluster.

1. Click on the `DocumentDb Test Plan` element. 
   May need to change user variables such as `CONNECTION_STRING`, `USERNAME`, and `PASSWORD` depending on SSH tunnel setup and target cluster.

1. Run the test plan.

### Run Test Plan with Other JDBC Driver

- It may be useful to run the tests against another data source to confirm expected functionality. 
  The user variables `CONNECTION_STRING`, `USERNAME`, and `PASSWORD` 
  can be changed to be used with another driver.

- Dependencies of the other driver may need to be setup and/or added to the JMeter `/lib` folder. 
  Refer to [JMeter documentation](https://jmeter.apache.org/usermanual/get-started.html#opt_jdbc) 
  and documentation of the specific database vendor for more information.

## Add Tests

### Add Test Data 

- Add test data as a `JSON` file under the `/testData` folder. 
  Use [MongoDB Extended JSON syntax](https://docs.mongodb.com/manual/reference/mongodb-extended-json/) 
  to explicitly specify types. 

### Add Test Cases

- To add a query, you need to add a new row in `Test_Plan.csv`. 

- Populate the `query` column with the query to be executed, the `result` column with the 
  expected result set, and the `test_name` column with a descriptive name for the query.
  
- To keep results uniform across different data sources for easier comparisons:

    - Columns are ordered explicitly instead of using `SELECT *`  so column order is deterministic.

    - Rows are ordered explicitly with `ORDER BY` so row order is deterministic.

    - Calculated fields are named explicitly with `AS` so they always use same name.