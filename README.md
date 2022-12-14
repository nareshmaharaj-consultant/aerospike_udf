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

#### Project
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

#### Running Data Loader
Run from the command line:
```java 
java -jar out/artifacts/app/udf.jar
```
By default and based on the default.properties file, a random selection of data for 10,000 records will be written to the namespace test and set financialdata in the database.

#### Running Data Reader
Change the app to reader mode by editing the default.properties file section below.
```bash
# ------- Run --------- #
app=reader
```

Now run the application again.
```java 
java -jar out/artifacts/app/udf.jar
```
As we have not configured the reader it will start a demo with default settings
 - run 100 client connection threads
 - creating client jobs where jobs will either be 
   - computing additional value-added fields
   - streaming data using UDFs to produce aggregated results

#### Results
Results explained below and these lines can be muted in the config file.
Consider sales lines are made up of significant fields
 - country - where the sale took place
 - segment - what part of industry does it refer to
 - product - what was sold

In window 2: Run the following query to show a sample.
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
Here are some of the results from the reader output.

```bash
Compute Profit bins for [C] Morocco in 1008 ms.
Total VAT for [C] Morocco, £190,999.51 in 9 ms.
Total SALES for [CS] Germany/Sport, £65,224.00 in 4 ms.
No results for [CSP] India/Government/Connect for JMS
```
Compute is creating value added fields to the data such as queryField, totalSales, totalCost, profit, profitMargin, taxRates and taxDue
We will discuss the field queryField in more detail further down.
```bash
SELECT  *  FROM test.financialdata  WHERE country = "Morocco"
+--------------------+-----------+--------------------------------+-----------+----------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
| segment            | country   | product                        | unitsSold | mfgPrice | salesPrice | date         | queryField                                                                                                                       | totalSales | totalCost | profit  | profitMargin | taxRates | taxDue            |
+--------------------+-----------+--------------------------------+-----------+----------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
| "Small Business"   | "Morocco" | "Cloud"                        | 252       | 1        | 147        | "01/10/2013" | MAP('{"MAR/CS":"Morocco/Small Business", "MAR/CSP":"Morocco/Small Business/Cloud", "MAR/C":"Morocco"}')                          | 37044      | 252       | 36792   | 99           | 20       | 6132              |
| "Government"       | "Morocco" | "Aerospike Cloud"              | 1397      | 0        | 6          | "01/03/2014" | MAP('{"MAR/CS":"Morocco/Government", "MAR/CSP":"Morocco/Government/Aerospike Cloud", "MAR/C":"Morocco"}')                        | 8382       | 0         | 8382    | 100          | 20       | 1397              |
| "Midmarket"        | "Morocco" | "Connect for Spark"            | 1948      | 204      | 110        | "01/01/2014" | MAP('{"MAR/CS":"Morocco/Midmarket", "MAR/CSP":"Morocco/Midmarket/Connect for Spark", "MAR/C":"Morocco"}')                        | 214280     | 397392    | -183112 | -86          | 20       | 0                 |
| "Enterprise"       | "Morocco" | "Aerospike SQL"                | 1897      | 3        | 6          | "01/04/2014" | MAP('{"MAR/CS":"Morocco/Enterprise", "MAR/CSP":"Morocco/Enterprise/Aerospike SQL", "MAR/C":"Morocco"}')                          | 11382      | 5691      | 5691    | 50           | 20       | 948.5             |
| "Enterprise"       | "Morocco" | "Connect for Presto"           | 49        | 18       | 105        | "01/11/2013" | MAP('{"MAR/CS":"Morocco/Enterprise", "MAR/CSP":"Morocco/Enterprise/Connect for Presto", "MAR/C":"Morocco"}')                     | 5145       | 882       | 4263    | 82           | 20       | 710.5             |
| "Small Business"   | "Morocco" | "Aerospike Tools"              | 504       | 0        | 7          | "01/10/2014" | MAP('{"MAR/CS":"Morocco/Small Business", "MAR/CSP":"Morocco/Small Business/Aerospike Tools", "MAR/C":"Morocco"}')                | 3528       | 0         | 3528    | 100          | 20       | 588               |
| "Small Business"   | "Morocco" | "Connect for Spark"            | 538       | 54       | 114        | "01/12/2014" | MAP('{"MAR/CS":"Morocco/Small Business", "MAR/CSP":"Morocco/Small Business/Connect for Spark", "MAR/C":"Morocco"}')              | 61332      | 29052     | 32280   | 52           | 20       | 5380              |
| "Small Business"   | "Morocco" | "Aerospike on AWS"             | 129       | 7        | 5          | "01/09/2014" | MAP('{"MAR/CS":"Morocco/Small Business", "MAR/CSP":"Morocco/Small Business/Aerospike on AWS", "MAR/C":"Morocco"}')               | 645        | 903       | -258    | -40          | 20       | 0                 |
| "Channel Partners" | "Morocco" | "Connect for Presto"           | 282       | 80       | 7          | "01/10/2014" | MAP('{"MAR/CS":"Morocco/Channel Partners", "MAR/CSP":"Morocco/Channel Partners/Connect for Presto", "MAR/C":"Morocco"}')         | 1974       | 22560     | -20586  | -1043        | 20       | 0                 |
| "Channel Partners" | "Morocco" | "Dynamic Cluster Management"   | 1564      | 5        | 105        | "01/07/2014" | MAP('{"MAR/CS":"Morocco/Channel Partners", "MAR/CSP":"Morocco/Channel Partners/Dynamic Cluster Management", "MAR/C":"Morocco"}') | 164220     | 7820      | 156400  | 95           | 20       | 26066.66666666666 |
+--------------------+-----------+--------------------------------+-----------+----------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
```

