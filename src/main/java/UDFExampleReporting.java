import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Language;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.task.RegisterTask;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class UDFExampleReporting {

    static int numberOfClientsReaders   = 10;
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

    public static void main( String args [])
    {
        if ( args.length != 3 ){
            System.out.println( "Enter: Country, Segment and Product\ne.g.: France, Retail, \"Connect for JMS\"");
            System.exit(0);
        }

        String country = null; if ( args.length> 0  ) country = args[0];
        String segment = null; if ( args.length> 0  ) segment = args[1];
        String product = null; if ( args.length> 0  ) product = args[2];

        try {
            loadProperties();
            udfReader = new UDFExampleReporting();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        udfReader.registerLua();

        /* [ Start client threads - reading data ] */
        for (int i = 0; i < numberOfClientsReaders; i++ )
        {
            RunnableReader R1 =
                    new RunnableReader(
                            "Aerospike Connection: ".concat(Integer.toString(i)),
                            monitor,
                            namespace,
                            set,
                            hosts,
                            luaConfigSourcePath,
                            clientPolicy,
                            country,
                            segment,
                            product
                    );
            R1.start();
        }
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
    }

    private static Host[] getHosts(String [] listOfIps) {
        Host[] tmpHost = new Host[listOfIps.length];
        for (int i = 0; i < listOfIps.length; i++) {
            tmpHost[i] =  new Host(listOfIps[i], port);
        }
        return tmpHost;
    }
}