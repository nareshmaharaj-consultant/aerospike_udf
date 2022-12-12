import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Language;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.task.RegisterTask;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import static java.lang.Thread.sleep;

public class UDFExampleReporting {

    static int numberOfClientsReaders   = 10;
    static long timeForJobMs            = 1000L;
    static long delayBetweenJobMs       = 1000L;
    static int port                     = 3000;
    static Monitor monitor              = new Monitor();
    static Host[] hosts                 = new Host[] {new Host("127.0.0.1", port)};

    static AerospikeClient client;
    static String namespace             = "test";
    static String set                   = "set1";
    static String user                  = "admin";
    static String pwd                   = "admin";
    static AuthMode authmode            = AuthMode.INTERNAL;

    static UDFExampleReporting udfReader = null;
    static String luaExamplePath        = "example.lua";
    static String luaConfigSourcePath   = ".";
    static ClientPolicy clientPolicy = null;

    public UDFExampleReporting(){
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = user;
        clientPolicy.password = pwd;
        clientPolicy.authMode = authmode;
        this.clientPolicy = clientPolicy;

        AerospikeClient client = new AerospikeClient(clientPolicy, hosts);
        this.client= client;
    }

    public static void main( String args []) throws InterruptedException {
        if ( args.length < 1 ){
            System.out.println( "" +
                    "Enter: Country, Segment and Product\n" +
                    "e.g.: France Retail \"Connect for JMS\"");
            System.exit(0);
        }

        String country = null; if ( args.length> 0  ) country = args[0];
        String segment = null; if ( args.length> 1  ) segment = args[1];
        String product = null; if ( args.length> 2  ) product = args[2];

        try {
            loadProperties();
            udfReader = new UDFExampleReporting();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        udfReader.registerLua();
        Vector<RunnableReader> readers = new Vector<>();

//        OperationJob operationJob = new OperationJob(
//                RunnableReader.OPERATION_TYPE_COMPUTE,
//                RunnableReader.QF_COUNTRY,
//                RunnableReader.OPERATION_REPORT_LABEL_NONE,
//                delayBetweenJobMs
//        );

        OperationJob operationJob = new OperationJob(
                RunnableReader.OPERATION_TYPE_QUERY,
                RunnableReader.QF_COUNTRY,
                RunnableReader.OPERATION_REPORT_LABEL_SALES,
                delayBetweenJobMs
        );

        AerospikeConnectionDetails aerospikeConnectionDetails = new AerospikeConnectionDetails(
                "Aerospike Connection",
                monitor,
                namespace,
                set,
                hosts,
                luaConfigSourcePath,
                clientPolicy
        );

        /* [ Start client threads - reading data ] */
        for (int i = 0; i < numberOfClientsReaders; i++ )
        {
            aerospikeConnectionDetails.setName("Aerospike Connection: ".concat(Integer.toString(i)));
            RunnableReader R1 =
                    new RunnableReader(
                            aerospikeConnectionDetails,
                            operationJob,
                            country,
                            segment,
                            product
                    );
            R1.start();
            readers.add(R1);
        }
        sleep(timeForJobMs);
        Iterator<RunnableReader> itr = readers.iterator();

        while( itr.hasNext() )
            itr.next().setRunning(false);

        monitor.waitTillComplete();
    }

    private UDFExampleReporting registerLua() {
        RegisterTask task = client.register(
                null,
                luaExamplePath,
                "example.lua", Language.LUA);
        // Poll cluster for completion every second for a maximum of 10 seconds.
        task.waitTillComplete(1000, 10000);
        return this;
    }

    private static void loadProperties() throws IOException {
        Properties defaultProps = new Properties();
        String profile = "default.properties";
        String propertyFile = profile;
        FileInputStream in = new FileInputStream(propertyFile);
        defaultProps.load(in);
        in.close();

        numberOfClientsReaders = Integer.parseInt(defaultProps.getProperty("numberOfClientsReaders"));
        namespace = defaultProps.getProperty("namespace");
        set = defaultProps.getProperty("set");
        hosts = getHosts( defaultProps.getProperty("hosts").split(",")) ;
        luaExamplePath = defaultProps.getProperty("luaExamplePath");
        luaConfigSourcePath = defaultProps.getProperty("luaConfigSourcePath");
        user = defaultProps.getProperty("user");
        pwd = defaultProps.getProperty("password");
        String auth = defaultProps.getProperty("authMode");
        if ( AuthMode.valueOf(auth) != null )
            authmode = AuthMode.valueOf(auth);
        timeForJobMs = Long.parseLong(defaultProps.getProperty("timeForJobMs"));
        delayBetweenJobMs = Long.parseLong(defaultProps.getProperty("delayBetweenJobMs"));
    }
    private static Host[] getHosts(String [] listOfIps) {
        Host[] tmpHost = new Host[listOfIps.length];
        for (int i = 0; i < listOfIps.length; i++) {
            tmpHost[i] =  new Host(listOfIps[i], port);
        }
        return tmpHost;
    }
}

/*
        // Compute
        RunnableReader.OPERATION_TYPE_COMPUTE,
        RunnableReader.QF_COUNTRY,
        RunnableReader.OPERATION_REPORT_LABEL_NONE

        RunnableReader.OPERATION_TYPE_COMPUTE,
        RunnableReader.QF_COUNTRY_SEGMENT,
        RunnableReader.OPERATION_REPORT_LABEL_NONE

        RunnableReader.OPERATION_TYPE_COMPUTE,
        RunnableReader.QF_COUNTRY_SEGMENT_PRODUCT,
        RunnableReader.OPERATION_REPORT_LABEL_NONE

        // Query
        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY,
        RunnableReader.OPERATION_REPORT_LABEL_SALES

        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY,
        RunnableReader.OPERATION_REPORT_LABEL_VAT

        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY_SEGMENT,
        RunnableReader.OPERATION_REPORT_LABEL_SALES

        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY_SEGMENT,
        RunnableReader.OPERATION_REPORT_LABEL_VAT

        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY_SEGMENT_PRODUCT,
        RunnableReader.OPERATION_REPORT_LABEL_SALES

        RunnableReader.OPERATION_TYPE_QUERY,
        RunnableReader.QF_COUNTRY_SEGMENT_PRODUCT,
        RunnableReader.OPERATION_REPORT_LABEL_VAT
     */
class OperationJob
{
    int operation;
    String queryType;
    int reportLabel;
    long delayBetweenJobMs;

    public int getOperation() {
        return operation;
    }

    public String getQueryType() {
        return queryType;
    }

    public int getReportLabel() {
        return reportLabel;
    }

    public long getDelayBetweenJobMs() {
        return delayBetweenJobMs;
    }

    public OperationJob(int operation, String queryType, int reportType, long delayBetweenJobMs)
    {
        this.operation = operation;
        this.queryType = queryType;
        this.reportLabel = reportType;
        this.delayBetweenJobMs = delayBetweenJobMs;
    }
}

class AerospikeConnectionDetails{
    private String name;

    public String getName() {
        return name;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSet() {
        return set;
    }

    public String getLuaConfigSourcePath() {
        return luaConfigSourcePath;
    }

    public ClientPolicy getClientPolicy() {
        return clientPolicy;
    }

    public Host[] getHosts() {
        return hosts;
    }

    private final Monitor monitor;
    private final String namespace;
    private final String set;
    private final String luaConfigSourcePath;
    private final ClientPolicy clientPolicy;
    private final Host[] hosts;

    public AerospikeConnectionDetails(
            String name, Monitor monitor, String namespace,
            String set, Host[] hosts, String luaConfigSourcePath,
            ClientPolicy clientPolicy
    )
    {
        this.name = name;
        this.monitor = monitor;
        this.namespace = namespace;
        this.set = set;
        this.hosts = hosts;
        this.luaConfigSourcePath  = luaConfigSourcePath;
        this.clientPolicy = clientPolicy;
    }

    public String setName(String name) {
        this.name = name;
        return name;
    }
}