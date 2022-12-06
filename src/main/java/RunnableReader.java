import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;

class RunnableReader implements Runnable {
    private final String namespace;
    private final String set;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;
    private String luaConfigSourcePath  = ".";

    RunnableReader(String name, Monitor monitor, String namespace, String set, Host[] hosts, String luaConfigSourcePath){
        this.namespace = namespace;
        this.set = set;
        this.threadName = name;
        this.monitor = monitor;
        this.client = new AerospikeClient(null, hosts );
        this.luaConfigSourcePath = luaConfigSourcePath;
    }

    @Override
    public void run()
    {
        calculateFigures();

        Object result = calculateRegionalVat(new String());
        System.out.println("TotalVat = " + result);

        result = calculateRegionalVat(new String("Germany"));
        System.out.println("TotalVat for DE = " + result);

        long from = 20000; long to = 30000;
        result = calculateRegionalVat(from,to);
        String out = String.format("TotalVat for sales between %d to %d = ",from, to);
        System.out.println(out + result);

        client.close();
        monitor.notifyComplete();
    }

    private void calculateFigures() {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        ExecuteTask et = client.execute(null,
                stmt,
                "example",
                "totals",
                Value.get("unitsSold"),
                Value.get("mfgPrice"),
                Value.get("salesPrice")
        );
        et.waitTillComplete();
    }

    private Object calculateRegionalVat(String region) {
        Filter filter = null;
        if ( region.length() > 0 ){
            filter = Filter.equal("country", region);
        }
        return totalRegionalVat( filter );
    }

    private Object calculateRegionalVat(long salesFromRange, long salesToRange) {
        Filter filter = Filter.range("totalSales", salesFromRange, salesToRange);
        return totalRegionalVat( filter );
    }

    private Object totalRegionalVat(Filter filter) {
        LuaConfig.SourceDirectory = luaConfigSourcePath;
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter( filter );

        ResultSet resultSet = client.queryAggregate(null, stmt, "example", "calculateVatDue");

        if (resultSet.next()) {
            Object result = resultSet.getObject();
            return result;
        }
        return null;
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}
