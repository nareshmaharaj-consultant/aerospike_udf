# Aerospike UDFs

### User Defined Functions

User-Defined Functions are functions that run on the server side. User-Defined Functions are written in Lua. Lua is a powerful, fast, lightweight, embeddable scripting language.
To learn more about User-Defined-Functions visit the 
![Aerospike Developer Hub](https://developer.aerospike.com/udf/knowing_lua). 
This repo hosts a small application to process sales data lines and provide insights on the data. 
It's purpose is educational. Some of Aerospike features used are as follows

  - Aggregation Streaming using application level Filtering
  - Filtering data in a UDF using Lua
  - Updating records and adding new computed fields in a UDF.
  - Using Expressions to limit the records touched.


###Overview of source files

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

Open up 2 shell windows. Run an instance of the Aerospike Database in docker container with the following command

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
