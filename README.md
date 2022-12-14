# Aerospike UDFs

### User Defined Functions

User-Defined Functions are functions that run on the server side. User-Defined Functions are written in Lua. Lua is a powerful, fast, lightweight, embeddable scripting language.
To learn more about User-Defined-Functions visit the 
 - https://developer.aerospike.com/udf/knowing_lua 

This repo hosts a small application to process sales data lines and provide insights on the data. 
It's purpose is educational. Some of the Aerospike features used in this example are as listed as follows

  - Aggregation Streaming - 
    - application level filtering 
       - using Filter
       - modelling & secondary Indexes with Map Values
  - UDF level Filtering 
      - using streams
  - Updating records with server side computed fields using UDF.
  - Using Expressions to limit the records touched.

### Overview of source files

```UDFExampleDataLoader```
  - Used for loading the sample sales data
  - there is a main class 
  - configure it from the property file under section *Loader*
  - data can be randomised
  - will call RunnableLoader thread class to simulate several clients loading data
 
```UDFExampleReporting```
  - Used for reading data and calling the UDFs
  - there is a main class
  - configure it from the property file under section *Reader*
  - UDFs will use Aggregation, Filters and Record level updates
  - will call RunnableReader or RunnableReaderFilter, the difference being the latter will use UDF filters in the streaming.
  
```Sales Data```
  - this class will make random sales data for you.
  - it's configurable in the property file under section *Reader*

### Getting Started

#### Docker

Open up 3 shell windows. Run an instance of the Aerospike Database in docker container with the following command

In window 1: Run the docker command
```bash
docker run -d -e "NAMESPACE=test" --name aerospike -p 3000-3002:3000-3002 -v aerospike-etc-6-1:/opt/aerospike/etc/ -v aerospike-data-6-1:/opt/aerospike/data aerospike:ce-6.1.0.3
```
In window 1: Tail the log file using
```bash
docker logs aerospike -f
```
In window 2: Log into the command line interactive shell for Aerospike call aql.
```bash
docker run -ti aerospike/aerospike-tools:latest aql -h  $(docker inspect -f '{{.NetworkSettings.IPAddress }}' aerospike)
```
In Window 3: We will run the writer application being the dataloader.
The directory stucture should resemble
```bash 
├── README.md
├── data
│   └── FinancialSample.csv
├── default.properties
├── lua
│   └── example.lua
├── out
│   └── artifacts
│       └── app
│           └── udf.jar
├── pom.xml
├── src
│   ├── main
│   │   ├── java
│   │   │   ├── META-INF
│   │   │   │   └── MANIFEST.MF
│   │   │   ├── Main.java
│   │   │   ├── RunnableLoader.java
│   │   │   ├── RunnableReader.java
│   │   │   ├── RunnableReaderFilter.java
│   │   │   ├── SalesData.java
│   │   │   ├── UDFExampleDataLoader.java
│   │   │   ├── UDFExampleReporting.java
│   │   │   └── UDFPlayground.java
│   │   └── resources
│   └── test
│       └── java
```

Run from the command line:
```java 
java -jar out/artifacts/app/udf.jar
```
By default and based on the default.properties file, a random selection of data for 10,000 records will be written to the namespace test and set financialdata in the database.

Change the app to reader mode by editing the default.properties file section below.
```bash
# ------- Run --------- #
app=reader
```

Now run the application again.
```java 
java -jar out/artifacts/app/udf.jar
```
As we have not configured the reader it will start doing the following
 - running 100 client threads
 - creating client jobs where jobs will either be 
   - computing additional fields
    - streaming data using UDFs to produce aggregated results
    
Results explained below and these lines can also be muted in the config file.
Consider that the sales lines are made up of
 - country - where the sale took place
 - segment - what part of industry does it refer to
 - product - what was sold

In window 2: Run the following query
```bash
SELECT segment,country,product,unitsSold,mfgPrice,salesPrice,date  FROM test.financialdata  WHERE country = "Italy"
+--------------------+----------+-------------------------------------+------------+-----------+-------------+--------------+
| segment            | country  | product                             | unitsSold  | mfgPrice  | salesPrice  | date         |
+--------------------+----------+-------------------------------------+------------+-----------+-------------+--------------+
| "Retail"           | "Italy"  | "Connect for Spark"                 | 877        | 7         | 117         | "01/12/2014" |
| "Retail"           | "Italy"  | "Aerospike Real-time Data Platform" | 1461       | 97        | 13          | "01/04/2014" |
| "Enterprise"       | "Italy"  | "Connect for ESP"                   | 547        | 78        | 9           | "01/05/2014" |
| "Channel Partners" | "Italy"  | "Aerospike Cloud Managed Service"   | 121        | 0         | 1           | "01/10/2014" |
| "Government"       | "Italy"  | "Real-Time Engine"                  | 471        | 8         | 3           | "01/10/2014" |
| ...
+--------------------+----------+-------------------------------------+------------+-----------+-------------+--------------+
```