import com.aerospike.client.*;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UDFExampleRandomDataLoaderSalesLines {

    static int startKeyFrom             = 0;
    static int numberOfClientsLoaders   = 10;
    static int numberOfRecords          = 10000000;
    static int batchWriteLump           = 1000;
    static int port                     = 3000;
    static Monitor monitor              = new Monitor();
    static Host[] hosts                 = new Host[] {new Host("127.0.0.1", port)};
    static String dataFilePath          = "sample.cvs";
    static String luaExamplePath        = "example.lua";
    static String luaConfigSourcePath   = ".";
    static boolean truncateBeforeStarting;

    static AerospikeClient client;
    static String namespace             = "test";
    static String set                   = "set1";
    static UDFExampleRandomDataLoaderSalesLines udf = null;
    static SalesData [] salesDataSeed700 = new SalesData[700];


    public UDFExampleRandomDataLoaderSalesLines() throws Exception {
        AerospikeClient client = new AerospikeClient(null, hosts);
        this.client= client;
        registerLua();
        createIndex(namespace, set, "country", IndexType.STRING );
        createIndex(namespace, set, "totalSales", IndexType.NUMERIC );

        if ( truncateBeforeStarting )
            client.truncate(null, namespace, set, null);
    }

    public static void main( String args [])
    {
        try {
            loadProperties();
            udf = new UDFExampleRandomDataLoaderSalesLines();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        /* [ Summary ] */
        int numberOfRecordsPerThread = numberOfRecords / numberOfClientsLoaders;
        long startTime = System.currentTimeMillis();

        System.out.println(
                "version 1\n" +
                        "NumberOfClient Threads:" + numberOfClientsLoaders +
                        "\nNumberOfRecords to Insert:" + numberOfRecords +
                        "\nStarting key:" + startKeyFrom
        );


        /* [ Load seed data we can play off ] */
        loadData();


        /* [ Start client threads - ingest data ] */
        for (int i = 0; i < numberOfClientsLoaders; i++ )
        {
            RunnableLoader R1 =
                    new RunnableLoader(
                            "Aerospike Connection: ".concat(Integer.toString(i)),
                            numberOfRecordsPerThread,
                            i * numberOfRecordsPerThread + startKeyFrom,
                            monitor,
                            namespace,
                            set,
                            hosts,
                            salesDataSeed700
                    );
            R1.start();
        }
        monitor.waitTillComplete();
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken " + ( timeTaken / 1 ) + " ms for " + numberOfRecords + " records.");

    }

    private static void loadProperties() throws IOException {
        Properties defaultProps = new Properties();
        String profile = "default.properties";
        String propertyFile = profile;
        FileInputStream in = new FileInputStream(propertyFile);
        defaultProps.load(in);
        in.close();

        numberOfClientsLoaders = Integer.parseInt(defaultProps.getProperty("numberOfClientsLoaders"));
        numberOfRecords = Integer.parseInt(defaultProps.getProperty("numberOfRecords"));
        batchWriteLump = Integer.parseInt(defaultProps.getProperty("batchWriteLump"));
        startKeyFrom = Integer.parseInt(defaultProps.getProperty("startKeyFrom"));
        namespace = defaultProps.getProperty("namespace");
        set = defaultProps.getProperty("set");
        hosts = getHosts( defaultProps.getProperty("hosts").split(",")) ;
        dataFilePath = defaultProps.getProperty("dataFilePath");
        luaExamplePath = defaultProps.getProperty("luaExamplePath");
        luaConfigSourcePath = defaultProps.getProperty("luaConfigSourcePath");
        truncateBeforeStarting = Boolean.parseBoolean(defaultProps.getProperty("truncateBeforeStarting"));
    }

    private static Host[] getHosts(String [] listOfIps) {
        Host[] tmpHost = new Host[listOfIps.length];
        for (int i = 0; i < listOfIps.length; i++) {
            tmpHost[i] =  new Host(listOfIps[i], port);
        }
        return tmpHost;
    }

    public static List<List<String>> loadData() {
        int i=0;
        String path = dataFilePath;
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if ( line.startsWith("#") )
                    continue;
                line = line.replaceAll("\"","");
                line = line.replace(" ", "");
                line = line.replaceAll("\\$","");
                String[] values = line.split(",");
                SalesData sd = new SalesData(values);
                salesDataSeed700[i++] = sd;
                records.add(Arrays.asList(values));
            }
            return records;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private UDFExampleRandomDataLoaderSalesLines registerLua() {
        RegisterTask task = client.register(
                null,
                luaExamplePath,
                "example.lua", Language.LUA);
        // Poll cluster for completion every second for a maximum of 10 seconds.
        task.waitTillComplete(1000, 10000);
        return this;
    }

    public void createIndex(String ns, String set, String bin, IndexType type ) throws Exception {
        IndexTask task = client.createIndex(null, ns, set,  bin.concat("_idx"), bin, type);
        task.waitTillComplete();
        int status = task.queryStatus();
    }
}