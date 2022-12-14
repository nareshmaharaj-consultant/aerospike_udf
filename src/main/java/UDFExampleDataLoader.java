import com.aerospike.client.*;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UDFExampleDataLoader {

    static int startKeyFrom             = 0;
    static int numberOfClientsLoaders   = 10;
    static int numberOfRecords          = 10000000;
    static int batchWriteLump           = 1000;
    static int port                     = 3000;
    static Monitor monitor              = new Monitor();
    static Host[] hosts                 = new Host[] {new Host("127.0.0.1", port)};
    static String dataFilePath          = "sample.cvs";
    static boolean truncateBeforeStarting;

    static AerospikeClient client;
    static String namespace             = "test";
    static String set                   = "set1";
    static String user                  = "admin";
    static String pwd                   = "admin";
    static AuthMode authmode            = AuthMode.INTERNAL;
    static UDFExampleDataLoader udf     = null;
    static SalesData [] salesDataSeed700 = new SalesData[700];

    static ClientPolicy clientPolicy = null;
    static AtomicInteger numberofRecordsWritten = new AtomicInteger();
    static AtomicInteger oldCoounter = new AtomicInteger();

    public UDFExampleDataLoader() throws Exception {
    }

    public void main() throws InterruptedException {
        try {
            loadProperties();

            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.user = user;
            clientPolicy.password = pwd;
            clientPolicy.authMode = authmode;
            this.clientPolicy = clientPolicy;
            AerospikeClient client = new AerospikeClient(clientPolicy, hosts);
            this.client= client;

            deleteIndex(namespace, set, "country");
            deleteIndex(namespace, set, "totalSales");
            deleteIndex(namespace, set, "queryField");
            createIndex(namespace, set, "country", IndexType.STRING );
            createIndex(namespace, set, "totalSales", IndexType.NUMERIC );
            createIndexList("queryFieldKeys",namespace, set, "queryField", IndexType.STRING, IndexCollectionType.MAPKEYS );
            createIndexList("queryFieldVals", namespace, set, "queryField", IndexType.STRING, IndexCollectionType.MAPVALUES );

            if ( truncateBeforeStarting )
                client.truncate(null, namespace, set, null);

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

        String statDesc = "Current Rate of writes/sec: ";
        Timer timer = new Timer();
        timer.schedule( new TimerTask() {
            public void run() {
                System.out.println ( statDesc + (numberofRecordsWritten.get() - oldCoounter.get()) );
                oldCoounter.set( numberofRecordsWritten.get());
            }
        }, 0, 1*1000);


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
                            salesDataSeed700,
                            clientPolicy,
                            numberofRecordsWritten
                    );
            R1.start();
        }

        monitor.waitTillComplete();
        timer.cancel();
        System.out.println ( statDesc + (numberofRecordsWritten.get() - oldCoounter.get()) );
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "Total Elapsed TimeTaken " + ( timeTaken / 1 ) + " ms for " + numberOfRecords + " records.");
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
        user = defaultProps.getProperty("user");
        pwd = defaultProps.getProperty("password");
        String auth = defaultProps.getProperty("authMode");
        if ( AuthMode.valueOf(auth) != null )
            authmode = AuthMode.valueOf(auth);
        hosts = getHosts( defaultProps.getProperty("hosts").split(",")) ;
        dataFilePath = defaultProps.getProperty("dataFilePath");
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

    public void createIndex(String ns, String set, String bin, IndexType type ) throws Exception {
        IndexTask task = client.createIndex(null, ns, set,  bin.concat("_idx"), bin, type);
        task.waitTillComplete();
    }

    public void createIndexList(String name, String ns, String set, String bin, IndexType type, IndexCollectionType ict ) throws Exception {
        IndexTask task = client.createIndex(null, ns, set,  name, bin, type, ict);
        task.waitTillComplete();
    }

    public void deleteIndex(String ns, String set, String bin ) throws Exception {
        IndexTask task = client.dropIndex(null, ns, set,  bin.concat("_idx"));
        task.waitTillComplete();
    }
}