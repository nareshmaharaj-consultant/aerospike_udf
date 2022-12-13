import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Language;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.lua.LuaCache;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.task.RegisterTask;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

public class UDFExampleReporting {

    static int numberOfClientsReaders   = 10;
    static long timeForJobMs            = 1000L;
    static long delayBetweenJobMs       = 1000L;
    static int port                     = 3000;
    static Monitor monitor              = new Monitor();
    static Host[] hosts                 = new Host[] {new Host("127.0.0.1", port)};

    static String operationQueryType;
    static String operationQueryFilter;
    static String operationQueryReportLabel;
    static String country;
    static String segment;
    static String product;

    static String demoJobsCountryList;
    static boolean runAsDemo = false;
    static boolean staggerJobs = false;
    static int staggerMaxPeriod = 1000;

    static AerospikeClient client;
    static String namespace             = "test";
    static String set                   = "set1";
    static String user                  = "admin";
    static String pwd                   = "admin";
    static AuthMode authmode            = AuthMode.INTERNAL;

    static UDFExampleReporting udfReader = null;
    static String luaExamplePath         = "example.lua";
    static String luaConfigSourcePath    = ".";
    static ClientPolicy clientPolicy     = null;

    static final String OPERATION_TYPE_COMPUTE="compute";
    static final String OPERATION_TYPE_QUERY="query";
    static final String OPERATION_TYPE_QUERY_TAX="tax";
    static final String OPERATION_REPORT_LABEL_SALES="sales";

    public UDFExampleReporting() {
        ClientPolicy clientPolicy   = new ClientPolicy();
        clientPolicy.user           = user;
        clientPolicy.password       = pwd;
        clientPolicy.authMode       = authmode;
        this.clientPolicy           = clientPolicy;
        AerospikeClient client      = new AerospikeClient(clientPolicy, hosts);
        this.client                 = client;
    }

    public static void main( String args []) throws InterruptedException {
        /*
            Load the property file
            Load the lue files
         */
        try {
            loadProperties();
            udfReader = new UDFExampleReporting();
            udfReader.registerLua();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        /*
            Need to kill the readers sometime..
         */
        Vector<RunnableReader> readers = new Vector<>();

        /*
            Set up the job
         */
        int jobType = operationQueryType.equalsIgnoreCase(OPERATION_TYPE_COMPUTE) ?
                RunnableReader.OPERATION_TYPE_COMPUTE: RunnableReader.OPERATION_TYPE_QUERY;

        int jobReportLabel = operationQueryReportLabel.equalsIgnoreCase(OPERATION_REPORT_LABEL_SALES) ?
                RunnableReader.OPERATION_REPORT_LABEL_SALES:RunnableReader.OPERATION_REPORT_LABEL_VAT;

        String jobSelection = operationQueryFilter.toUpperCase(Locale.ROOT);

        OperationJob operationJobQueryCS = new OperationJob(
                jobType, jobSelection, jobReportLabel, delayBetweenJobMs,
                country, segment, product
        );

        /*
            Carries for connection details
         */
        AerospikeConnectionDetails aerospikeConnectionDetails = new AerospikeConnectionDetails(
                "Aerospike Connection",
                monitor,
                namespace,
                set,
                hosts,
                luaConfigSourcePath,
                clientPolicy
        );

        /*
            Start client threads - reading data
        */
        for (int i = 0; i < numberOfClientsReaders; i++ )
        {
            if ( runAsDemo ) {
                OperationJob operationJobDemo = getOperationJobDemo();
                operationJobQueryCS = operationJobDemo;
            }
            aerospikeConnectionDetails.setName("Aerospike Connection: ".concat(Integer.toString(i)));
            RunnableReader R1 =
                    new RunnableReader( aerospikeConnectionDetails, operationJobQueryCS );
            if ( staggerJobs)
                sleep( new Random().nextInt(staggerMaxPeriod) );
            R1.start();
            readers.add(R1);
        }
        sleep(timeForJobMs);
        Iterator<RunnableReader> itr = readers.iterator();

        while( itr.hasNext() )
            itr.next().setRunning(false);

        monitor.waitTillComplete();
    }

    private static OperationJob getOperationJobDemo() {

        int max=2; int min=1;
        /* decide on query type compute or query */
        int jobType = new Random().nextInt((max - min) + 1) + min;

        max=2; min=1;
        /* decide on job label type sales or vat */
        int jobReportLabel = new Random().nextInt((max - min) + 1) + min;

        max=3; min=1;
        /* decide on whether to do i.e. 1,2,3 # C=Country, CS= Country/Segment, CSP=Country/Segment/Product */
        int selectionDepth = new Random().nextInt((max - min) + 1) + min;
        String operationQueryFilter = "C";
        switch ( selectionDepth ){
            case 1:
                operationQueryFilter = "C";
                break;
            case 2:
                operationQueryFilter = "CS";
                break;
            case 3:
                operationQueryFilter = "CSP";
                break;
        }

        String queryFilterCountry = SalesData.getRandomCountry();
        if ( demoJobsCountryList != null && demoJobsCountryList.length() > 0 ){
            String [] countryDemoList = demoJobsCountryList.split(",");
            queryFilterCountry = countryDemoList[new Random().nextInt( countryDemoList.length) ];
        }

        String queryFilterSegment = SalesData.getRandomSegment();
        String queryFilterProduct = SalesData.getRandomProduct();

        OperationJob job = new OperationJob(jobType, operationQueryFilter, jobReportLabel, delayBetweenJobMs,
                queryFilterCountry, queryFilterSegment, queryFilterProduct );

        System.out.println( job );
        return job;
    }

    private UDFExampleReporting registerLua() {
        LuaCache.clearPackage("example");
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
        operationQueryType = defaultProps.getProperty("operationQueryType");
        operationQueryFilter = defaultProps.getProperty("operationQueryFilter");
        operationQueryReportLabel = defaultProps.getProperty("operationQueryReportLabel");
        country = defaultProps.getProperty("queryFilterCountry");
        segment = defaultProps.getProperty("queryFilterSegment");
        product = defaultProps.getProperty("queryFilterProduct");
        runAsDemo = Boolean.parseBoolean( defaultProps.getProperty("demoJobs") );
        demoJobsCountryList = defaultProps.getProperty("demoJobsCountryList");
        staggerJobs = Boolean.parseBoolean(defaultProps.getProperty("staggerJobs"));
        staggerMaxPeriod = Integer.parseInt(defaultProps.getProperty("staggerMaxPeriod"));
    }
    private static Host[] getHosts(String [] listOfIps) {
        Host[] tmpHost = new Host[listOfIps.length];
        for (int i = 0; i < listOfIps.length; i++) {
            tmpHost[i] =  new Host(listOfIps[i], port);
        }
        return tmpHost;
    }
}

class OperationJob {

    int operation;
    String queryType;
    int reportLabel;
    long delayBetweenJobMs;

    String country;
    String segment;
    String product;

    public void setCountry(String country) {
        this.country = country;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getCountry() {
        return country;
    }

    public String getSegment() {
        return segment;
    }

    public String getProduct() {
        return product;
    }

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

    @Override
    public String toString() {
        return "OperationJob{" +
                "operation=" + operation +
                ", queryType='" + queryType + '\'' +
                ", reportLabel=" + reportLabel +
                ", delayBetweenJobMs=" + delayBetweenJobMs +
                ", country='" + country + '\'' +
                ", segment='" + segment + '\'' +
                ", product='" + product + '\'' +
                '}';
    }

    public OperationJob(int operation, String queryType, int reportType, long delayBetweenJobMs,
                        String country, String segment,String product )
    {
        this.operation = operation;
        this.queryType = queryType;
        this.reportLabel = reportType;
        this.delayBetweenJobMs = delayBetweenJobMs;
        this.country = country;
        this.segment = segment;
        this.product = product;
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