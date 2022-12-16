

![img.png](out/artifacts/img3.png)



### Context

User-Defined Functions (UDF) are *functions* that run on the server side where the database hosts are located.
UDF(s) are written in Lua. Lua is a powerful, fast, lightweight, embeddable scripting language.
To learn more about User-Defined-Functions visit:  
 - https://developer.aerospike.com/udf/knowing_lua 

This repo hosts a small application to process sales data lines and provide insights on the data. 
It's purpose is educational. Some of the Aerospike features used in this example are listed below.

  - Aggregation Streaming
    - Application Level Filtering 
       - Using Aerospike Filters
       - Data Modelling & Secondary Indexes of Map Values Type
  - UDF Server Side Filtering 
      - Using Streams
  - Updating records with server side computed fields using UDF.
  - Using Aerospike Expressions to limit the number records touched.

### Overview of source files

```UDFExampleDataLoader```
  - Used for loading the sample data set
  - Configurable from the property file section *Loader*
  - Data can be randomised
  - Calls ```RunnableLoader``` class to simulate several clients loading data
 
```UDFExampleReporting```
  - Used for reading data and calling the UDFs
  - Configurable from the property file under section *Reader*
  - UDFs will use Aggregation, Filters and Record level updates
  - Calls ```RunnableReader``` or ```RunnableReaderFilter```, the difference being the latter will use UDF filters in the streaming.
  
```Sales Data```
  - Will produce random sales data.
  - Configurable in the property file under section *Reader*

### Getting Started

#### Docker

Open 3 separate shell windows. Run an instance of the Aerospike Database in a docker container with the following command

**Window 1**: Run the docker command
```bash
docker run -d -e "NAMESPACE=test" --name aerospike -p 3000-3002:3000-3002 -v aerospike-etc-6-1:/opt/aerospike/etc/ -v aerospike-data-6-1:/opt/aerospike/data aerospike:ce-6.1.0.3
```
**Window 1**: Tail the log file using
```bash
docker logs aerospike -f
```
**Window 2**: Log into the command line and use the interactive shell for Aerospike called aql.
```bash
docker run -ti aerospike/aerospike-tools:latest aql -h  $(docker inspect -f '{{.NetworkSettings.IPAddress }}' aerospike)
```

#### Project
**Window 3**: We are going to run the data loader instance.
The directory structure should resemble the following.
```text 
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
Running from the command line, it's assumes you have a Java Runtime Environment set up.
```java 
java -jar out/artifacts/app/udf.jar
```
Based on the default.properties file, a random selection of 10,000 
records will be written to the database in a namespace ```test``` and set called  ```financialdata```.
The application will echo to stdout the number of writes/second.

#### Running Data Reader
Change the app to reader mode by editing the ```default.properties``` file section below.
```bash
# ------- Run --------- #
app=reader
```

Now run the application again.
```java 
java -jar out/artifacts/app/udf.jar
```
Using the default settings the reader will startup in demo mode and do the following:
 - Spin up 100 client connection threads
 - Create a single client job per thread where ```OperationJob``` will either be a
   - Compute based ```OperationJob``` providing additional value-added fields per record
   - Streaming data using UDFs to produce aggregated results

#### Results
Results are explained below and these lines can be muted in the config file too.
Consider that sales lines are made up of significant fields
 - country - where the sale took place
 - segment - what part of industry does it refer to
 - product - what was sold

**Window 2**: Run the following query to show a sample of the data loader.
```text
SELECT segment,country,product,unitsSold,mfgPrice,salesPrice,date FROM test.financialdata  WHERE country = "Italy"
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

```text
Compute Profit bins for [C] Morocco in 1008 ms.
Total VAT for [C] Morocco, £190,999.51 in 9 ms.
Total SALES for [CS] Germany/Sport, £65,224.00 in 4 ms.
No results for [CSP] India/Government/Connect for JMS
```
#### Results Explained
*Compute* 
```text
Compute Profit bins for [C] Morocco in 1008 ms.
``` 
This creates additional value added bins named 
```queryField, totalSales, totalCost, profit, profitMargin, taxRates, taxDue```.
Later we will discuss the field ```queryField``` in more detail.

```text
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

*Total VAT* 
```text
Total VAT for [C] Morocco, £190,999.51 in 9 ms.
```
Taxes due on sales for all order(s) in the country Morocco. 
We know this becuase the single char [C] is telling us the aggregation report was produced purely at the country level.

*Total Sales*
```text
Total SALES for [CS] Germany/Sport, £65,224.00 in 4 ms.
```
This is the total number of sales made for the country Germany in the segment Sport. 
We also know this as [CS] is telling us the aggregation report was produced for country / segment filter.

*No Results*
```text
No results for [CSP] India/Government/Connect for JMS
```
As the reader class ```RunnableReader``` is generating the client jobs, some data in the query job may not
be present in the database so searching for it may yeild no results.
For e.g. we are trying to aggregate a report on all products sold in *India* for the market segment
*Government* for the product *"Connect for JMS"*.  If we query the database, we can see the data is not 
present. We can use our bin field ```queryField``` to 
verify this. 

```text
SELECT * FROM test.financialdata IN MAPVALUES WHERE queryField = "India/Government/Connect for JMS"
0 rows in set (0.012 secs)

OK
```
Clearly there is no data. Let's open up the filter ```queryField``` to allow more results.
```text
SELECT * FROM test.financialdata IN MAPVALUES WHERE queryField = "India/Government"
+--------------+---------+---------------------+-----------+----------+------------+--------------+-------------------------------------------------------------------------------------------------------+------------+-----------+--------+--------------+----------+--------+
| segment      | country | product             | unitsSold | mfgPrice | salesPrice | date         | queryField                                                                                            | totalSales | totalCost | profit | profitMargin | taxRates | taxDue |
+--------------+---------+---------------------+-----------+----------+------------+--------------+-------------------------------------------------------------------------------------------------------+------------+-----------+--------+--------------+----------+--------+
| "Government" | "India" | "Connect for Spark" | 167       | 53       | 1          | "01/11/2014" | MAP('{"IND/CS":"India/Government", "IND/CSP":"India/Government/Connect for Spark", "IND/C":"India"}') | 167        | 8851      | -8684  | -5200        | 20       | 0      |
+--------------+---------+---------------------+-----------+----------+------------+--------------+-------------------------------------------------------------------------------------------------------+------------+-----------+--------+--------------+----------+--------+
1 row in set (0.010 secs)

OK
```
We can now see in *India*, for the market segment *Government* that it only has 1 product line sold - *"Connect for Spark"*.

If we open this up even further, we can see all products sold in India.
```text
SELECT * FROM test.financialdata IN MAPVALUES WHERE queryField = "India"
+--------------------+---------+-----------------------------------+-----------+----------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
| segment            | country | product                           | unitsSold | mfgPrice | salesPrice | date         | queryField                                                                                                               | totalSales | totalCost | profit  | profitMargin | taxRates | taxDue            |
+--------------------+---------+-----------------------------------+-----------+----------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
| "Channel Partners" | "India" | "Aerospike Database"              | 1213      | 3        | 12         | "01/10/2014" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Aerospike Database", "IND/C":"India"}')       | 14556      | 3639      | 10917   | 75           | 20       | 1819.5            |
| "Midmarket"        | "India" | "Aerospike Database"              | 320       | 2        | 102        | "01/06/2014" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Aerospike Database", "IND/C":"India"}')                     | 32640      | 640       | 32000   | 98           | 20       | 5333.333333333334 |
| "Health"           | "India" | "Technology"                      | 488       | 241      | 5          | "01/06/2014" | MAP('{"IND/CS":"India/Health", "IND/CSP":"India/Health/Technology", "IND/C":"India"}')                                   | 2440       | 117608    | -115168 | -4720        | 20       | 0                 |
| "Retail"           | "India" | "Connect for Presto"              | 555       | 23       | 14         | "01/11/2014" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Connect for Presto", "IND/C":"India"}')                           | 7770       | 12765     | -4995   | -65          | 20       | 0                 |
| "Sport"            | "India" | "Features and Editions"           | 1722      | 28       | 300        | "01/10/2014" | MAP('{"IND/CS":"India/Sport", "IND/CSP":"India/Sport/Features and Editions", "IND/C":"India"}')                          | 516600     | 48216     | 468384  | 90           | 20       | 78064             |
| "Midmarket"        | "India" | "Connect for Pulsar"              | 873       | 109      | 18         | "01/06/2014" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Connect for Pulsar", "IND/C":"India"}')                     | 15714      | 95157     | -79443  | -506         | 20       | 0                 |
| "Health"           | "India" | "Aerospike Cloud Managed Service" | 122       | 27       | 79         | "01/09/2014" | MAP('{"IND/CS":"India/Health", "IND/CSP":"India/Health/Aerospike Cloud Managed Service", "IND/C":"India"}')              | 9638       | 3294      | 6344    | 65           | 20       | 1057.333333333333 |
| "Retail"           | "India" | "Hybrid Memory Architecture"      | 72        | 1        | 90         | "01/09/2013" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Hybrid Memory Architecture", "IND/C":"India"}')                   | 6480       | 72        | 6408    | 98           | 20       | 1068              |
| "Government"       | "India" | "Connect for Spark"               | 167       | 53       | 1          | "01/11/2014" | MAP('{"IND/CS":"India/Government", "IND/CSP":"India/Government/Connect for Spark", "IND/C":"India"}')                    | 167        | 8851      | -8684   | -5200        | 20       | 0                 |
| "Midmarket"        | "India" | "Cross Datacenter Replication"    | 1020      | 84       | 1          | "01/12/2014" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Cross Datacenter Replication", "IND/C":"India"}')           | 1020       | 85680     | -84660  | -8300        | 20       | 0                 |
| "Retail"           | "India" | "Dynamic Cluster Management"      | 381       | 39       | 97         | "01/10/2014" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Dynamic Cluster Management", "IND/C":"India"}')                   | 36957      | 14859     | 22098   | 59           | 20       | 3683              |
| "Sport"            | "India" | "Aerospike SQL"                   | 1132      | 3        | 2          | "01/10/2013" | MAP('{"IND/CS":"India/Sport", "IND/CSP":"India/Sport/Aerospike SQL", "IND/C":"India"}')                                  | 2264       | 3396      | -1132   | -50          | 20       | 0                 |
| "Channel Partners" | "India" | "Connect for Spark"               | 1097      | 4        | 12         | "01/11/2013" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Connect for Spark", "IND/C":"India"}')        | 13164      | 4388      | 8776    | 66           | 20       | 1462.666666666667 |
| "Retail"           | "India" | "Aerospike Cloud Managed Service" | 855       | 1        | 2          | "01/11/2013" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Aerospike Cloud Managed Service", "IND/C":"India"}')              | 1710       | 855       | 855     | 50           | 20       | 142.5             |
| "Midmarket"        | "India" | "Connect"                         | 48        | 1        | 2          | "01/10/2013" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Connect", "IND/C":"India"}')                                | 96         | 48        | 48      | 50           | 20       | 8                 |
| "Retail"           | "India" | "Connect for Presto"              | 1367      | 22       | 12         | "01/11/2013" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Connect for Presto", "IND/C":"India"}')                           | 16404      | 30074     | -13670  | -84          | 20       | 0                 |
| "Health"           | "India" | "Connect for Presto"              | 222       | 97       | 98         | "01/09/2014" | MAP('{"IND/CS":"India/Health", "IND/CSP":"India/Health/Connect for Presto", "IND/C":"India"}')                           | 21756      | 21534     | 222     | 1            | 20       | 37                |
| "Enterprise"       | "India" | "Connect for Kafka"               | 670       | 0        | 9          | "01/06/2014" | MAP('{"IND/CS":"India/Enterprise", "IND/CSP":"India/Enterprise/Connect for Kafka", "IND/C":"India"}')                    | 6030       | 0         | 6030    | 100          | 20       | 1005              |
| "Midmarket"        | "India" | "Document Database"               | 282       | 0        | 43         | "01/11/2013" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Document Database", "IND/C":"India"}')                      | 12126      | 0         | 12126   | 100          | 20       | 2021              |
| "Enterprise"       | "India" | "Aerospike SQL"                   | 1724      | 8        | 1          | "01/11/2013" | MAP('{"IND/CS":"India/Enterprise", "IND/CSP":"India/Enterprise/Aerospike SQL", "IND/C":"India"}')                        | 1724       | 13792     | -12068  | -700         | 20       | 0                 |
| "Health"           | "India" | "Connect for ESP"                 | 412       | 1        | 204        | "01/01/2014" | MAP('{"IND/CS":"India/Health", "IND/CSP":"India/Health/Connect for ESP", "IND/C":"India"}')                              | 84048      | 412       | 83636   | 99           | 20       | 13939.33333333333 |
| "Small Business"   | "India" | "Cloud"                           | 990       | 2        | 6          | "01/10/2014" | MAP('{"IND/CS":"India/Small Business", "IND/CSP":"India/Small Business/Cloud", "IND/C":"India"}')                        | 5940       | 1980      | 3960    | 66           | 20       | 660               |
| "Midmarket"        | "India" | "Aerospike Monitoring"            | 1020      | 35       | 93         | "01/05/2014" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Aerospike Monitoring", "IND/C":"India"}')                   | 94860      | 35700     | 59160   | 62           | 20       | 9860              |
| "Sport"            | "India" | "Aerospike Monitoring"            | 750       | 7        | 12         | "01/08/2014" | MAP('{"IND/CS":"India/Sport", "IND/CSP":"India/Sport/Aerospike Monitoring", "IND/C":"India"}')                           | 9000       | 5250      | 3750    | 41           | 20       | 625               |
| "Retail"           | "India" | "Connect for Presto"              | 1046      | 4        | 1          | "01/02/2014" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Connect for Presto", "IND/C":"India"}')                           | 1046       | 4184      | -3138   | -300         | 20       | 0                 |
| "Small Business"   | "India" | "Cross Datacenter Replication"    | 2595      | 1        | 2          | "01/10/2013" | MAP('{"IND/CS":"India/Small Business", "IND/CSP":"India/Small Business/Cross Datacenter Replication", "IND/C":"India"}') | 5190       | 2595      | 2595    | 50           | 20       | 432.5             |
| "Channel Partners" | "India" | "Connect for Spark"               | 218       | 0        | 6          | "01/05/2014" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Connect for Spark", "IND/C":"India"}')        | 1308       | 0         | 1308    | 100          | 20       | 218               |
| "Education"        | "India" | "Cross Datacenter Replication"    | 993       | 1        | 15         | "01/01/2014" | MAP('{"IND/CS":"India/Education", "IND/CSP":"India/Education/Cross Datacenter Replication", "IND/C":"India"}')           | 14895      | 993       | 13902   | 93           | 20       | 2317              |
| "Channel Partners" | "India" | "Aerospike on AWS"                | 806       | 1        | 94         | "01/08/2014" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Aerospike on AWS", "IND/C":"India"}')         | 75764      | 806       | 74958   | 98           | 20       | 12493             |
| "Midmarket"        | "India" | "Features and Editions"           | 495       | 22       | 3          | "01/12/2014" | MAP('{"IND/CS":"India/Midmarket", "IND/CSP":"India/Midmarket/Features and Editions", "IND/C":"India"}')                  | 1485       | 10890     | -9405   | -634         | 20       | 0                 |
| "Channel Partners" | "India" | "Connect for Kafka"               | 703       | 3        | 10         | "01/06/2014" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Connect for Kafka", "IND/C":"India"}')        | 7030       | 2109      | 4921    | 70           | 20       | 820.1666666666666 |
| "Health"           | "India" | "Aerospike Monitoring"            | 892       | 205      | 11         | "01/09/2013" | MAP('{"IND/CS":"India/Health", "IND/CSP":"India/Health/Aerospike Monitoring", "IND/C":"India"}')                         | 9812       | 182860    | -173048 | -1764        | 20       | 0                 |
| "Enterprise"       | "India" | "Features and Editions"           | 80        | 17       | 10         | "01/12/2013" | MAP('{"IND/CS":"India/Enterprise", "IND/CSP":"India/Enterprise/Features and Editions", "IND/C":"India"}')                | 800        | 1360      | -560    | -70          | 20       | 0                 |
| "Channel Partners" | "India" | "Aerospike Database"              | 898       | 2        | 3          | "01/09/2014" | MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Aerospike Database", "IND/C":"India"}')       | 2694       | 1796      | 898     | 33           | 20       | 149.6666666666667 |
| "Retail"           | "India" | "Dynamic Cluster Management"      | 615       | 8        | 10         | "01/02/2014" | MAP('{"IND/CS":"India/Retail", "IND/CSP":"India/Retail/Dynamic Cluster Management", "IND/C":"India"}')                   | 6150       | 4920      | 1230    | 20           | 20       | 205               |
+--------------------+---------+-----------------------------------+-----------+----------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------+------------+-----------+---------+--------------+----------+-------------------+
35 rows in set (0.012 secs)

OK
```

### Filtering

We are discussing 2 possibilities in which data can be filtered where filtering is based on more than 1 bin and 
where the intention of the query is get aggregated results returned.
- Filtering with a Secondary Index using Map Keys or Map Values
- Filtering in the UDF itself as part of the stream when aggregating.

Given the default behaviour is to use UDF filtering, let's checkout the implementation.

As mentioned previously, Filtering in this example is based on more than one bin as follows. 
If there was only 1 bin then much of what we are x would not be required. 

```segment            | country | product```

Switch off the demo mode feature.
```bash
# Run a sample of jobs based on number of numberOfClientsReaders
...
demoJobs=false
...
```
By disabling the demo mode, we revert to the default reader configuration which is 
an *"aggregation query, filtering on Country, Segment and Product in order to produce 
a tax aggregation report for Technology products sold in the Enterprise business segment
of Italy"*.

![img.png](out/artifacts/img.png)


```bash
# ------ Reader -------- #
numberOfClientsReaders=100
timeForJobMs=120000
delayBetweenJobMs=2000
useUDFFilterLogic=false

# compute or query
operationQueryType=query

# C=Country, CS= Country/Segment, CSP=Country/Segment/Product
operationQueryFilter=CSP

# sales or vat
operationQueryReportLabel=VAT

# Query filter values passed in
queryFilterCountry=Italy
queryFilterSegment=Enterprise
queryFilterProduct=Technology
```
You might have noticed the line below which reinforces we are using the UDF code for filtering.
```bash 
useUDFFilterLogic=true
```

#### UDF Filter code

The Lua file where the UDF is defined is found in the lua/ directory.
```bash
├── lua
│   └── example.lua
```

This is what the VAT(tax) aggregation code looks like. It first calls the filter function to 
determine if a record should be allowed to pass into the aggregate function. If so, the results from the aggregate are then sent to the reduce function.

```lua
-- STREAMS FUNCTIONS FOR AGGREGATION (Filter) --
function calculateVatDueFilter(stream, country, segment, product)
    return stream:filter(countrySegmentProductFilterClosure(country, segment, product))
            :aggregate( map{ country = nil }, aggregate_vatDue):reduce(reduce_stream)
end
```

Here is the filter function from class ```RunnableReaderFilter```which takes our 3 bin values ```country, segment and product```.
It is also checks for ```[country, segment, null]``` and also, ```[country, null, null]```.
```lua
-- FILTERS --
local function countrySegmentProductFilterClosure(country_arg, segment_arg, product_arg)
    local function countrySegmentProductFilter(rec)
       local country_rec = rec["country"]
       local segment_rec = rec["segment"]
       local product_rec = rec["product"]

        --if (not country_arg or type(country_arg) ~= "string") then country_arg = nil end
        if (not segment_arg or type(segment_arg) ~= "string") then segment_arg = nil end
        if (not product_arg or type(product_arg) ~= "string") then product_arg = nil end

        local retVal = false

        if (country_arg and country_arg == country_rec) then
            retVal = true
        else
            return false
        end

        if segment_arg ~= nil then
            if (segment_arg and segment_arg == segment_rec) then
                retVal = true
            else
                return false
            end
        end

        if product_arg ~= nil then
            if (product_arg and product_arg == product_rec) then
                retVal = true
            else
                return false
            end
        end

        return retVal
    end
    return countrySegmentProductFilter
end
```

In the application we are filtering on country regardless. Finally we call the method ```getAggregateQueryFilterResult(...)```
which returns the filtered aggregated data. It is worth noting that currently you **cannot** use 
an Aerospike Expression to do this. Although, that might change in the future.

```java
@Override
protected Object[] getAggregationReport(String queryField, String packageName, String functionName) {

        long startTime = System.currentTimeMillis();
        LuaConfig.SourceDirectory = luaConfigSourcePath;

        Filter f = Filter.equal("country", country);

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter( f );

        ResultSet resultSet = getAggregateQueryFilterResult( queryField, stmt, packageName, functionName );

        Iterator itr = resultSet.iterator();
        if (itr.hasNext()) {
            long endTime = System.currentTimeMillis();
            Long timeTaken = endTime - startTime;
            Object result = itr.next();
            return new Object[]{result, timeTaken};
        }
        return null;
    }

protected ResultSet getAggregateQueryFilterResult(String queryType, Statement stmt, String packageName, String functionName){
        if ( queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT_PRODUCT) )
            return  client.queryAggregate(null, stmt, packageName, functionName,
                Value.get(country), Value.get(segment), Value.get(product) );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT) )
            return  client.queryAggregate(null, stmt, packageName, functionName,
                Value.get(country), Value.get(segment), Value.getAsNull() );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY) )
            return client.queryAggregate(null, stmt, packageName, functionName,
                Value.get(country), Value.getAsNull(), Value.getAsNull() );;
        return null;
        }    
```
#### Filter with Secondary Indexes
To use Secondary Indexes we need to change the default behaviour of our filtering. Go ahead and 
set the field below to false. This will use the following class ```RunnableReader``` to produce the reports we need .

```bash
# ------ Reader -------- #
useUDFFilterLogic=false
```

Earlier, we mentioned about the bin ```queryField``` but we did not explain the data values in
```queryField```. The data is essentially a map of key:values. Below is an example
```bash
MAP('{"IND/CS":"India/Channel Partners", "IND/CSP":"India/Channel Partners/Aerospike Database", "IND/C":"India"}')
```
Or the easier formatted JSON version of the same
```json
{
  "IND/CS": "India/Channel Partners",
  "IND/CSP": "India/Channel Partners/Aerospike Database",
  "IND/C": "India"
}
```
So by now you should be familiar with the content. But to be sure let's discuss one of the lines.
```json
"IND/CSP": "India/Channel Partners/Aerospike Database"
```
- IND is the ISO3 code for India
- India is the country
- Channel Partners is the segment
- Aerospike Database is the product line

This means that we can search the bin independently for all records based on: 
- country
- country and segment
- or all three country, segment and product

```bash
aql> set output JSON
OUTPUT = JSON
aql> SELECT * FROM test.financialdata IN MAPVALUES WHERE queryField = "Italy/Enterprise/Aerospike on AWS"

[
    [
        {
          "segment": "Enterprise",
          "country": "Italy",
          "product": "Aerospike on AWS",
          "unitsSold": 1361,
          "mfgPrice": 2,
          "salesPrice": 228,
          "date": "01/12/2014",
          "queryField": {
            "ITA/CSP": "Italy/Enterprise/Aerospike on AWS",
            "ITA/CS": "Italy/Enterprise",
            "ITA/C": "Italy"
          },
          "totalSales": 310308,
          "totalCost": 2722,
          "profit": 307586,
          "profitMargin": 99,
          "taxRates": 20,
          "taxDue": 51264.333333333336
        }
    ],
    [
        {
          "Status": 0
        }
    ]
]
```
#### Secondary Indexes
To support this query we need have the correct indexes. In our case we need an index of type ```mapvalues```.
These are all created in the application code when the data is loaded using ```UDFExampleDataLoader```.
```bash
aql> set output TABLE
OUTPUT = TABLE
aql> show indexes
+--------+---------+-----------------+-------------+-----------------+-------+---------------------+-----------+
| ns     | context | bin             | indextype   | set             | state | indexname           | type      |
+--------+---------+-----------------+-------------+-----------------+-------+---------------------+-----------+
| "test" | "null"  | "country"       | "default"   | "financialdata" | "RW"  | "country_idx"       | "string"  |
| "test" | "null"  | "queryField"    | "mapvalues" | "financialdata" | "RW"  | "queryFieldVals"    | "string"  |
+--------+---------+-----------------+-------------+-----------------+-------+---------------------+-----------+
```

The class ```RunnableReader``` which used Secondary Indexes has the code snippet below 
which calls the aggregation. Note the filter being used is ```IndexCollectionType.MAPVALUES```.
```java
/**
     * Note, you cannot use expression filters with queryAggregate
     * QueryPolicy qPolicy = new QueryPolicy();
     * qPolicy.filterExp = Exp.build(expFilter);
     *
     * @param queryField
     * @return
     */
    protected Object[] getAggregationReport( String queryField, String packageName, String functionName ) {
        String queryFieldMapValue = getQueryFieldMapValue(queryField);
        Filter f = Filter.contains(QF_BIN_NAME, IndexCollectionType.MAPVALUES, queryFieldMapValue);

        long startTime = System.currentTimeMillis();
        LuaConfig.SourceDirectory = luaConfigSourcePath;

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter( f );
        stmt.setAggregateFunction(packageName, functionName );
        ResultSet resultSet = client.queryAggregate(null, stmt);

        Iterator itr = resultSet.iterator();
        if (itr.hasNext()) {
            long endTime = System.currentTimeMillis();
            Long timeTaken = endTime - startTime;
            Object result = itr.next();
            return new Object[]{result, timeTaken};
        }
        return null;
    }
```

If filtering in the UDF the ```queryField``` bin may not be required but consider as the dataset grows using a Secondary 
Index in conjunction with the ```queryField``` might provide better overall performance.

