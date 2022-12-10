import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.ExecuteTask;

import java.util.Iterator;
import java.util.Locale;

class RunnableReader implements Runnable {
    private final String namespace;
    private final String set;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;
    private String luaConfigSourcePath  = ".";
    private String country;
    private String segment;
    private String product;

    RunnableReader(String name, Monitor monitor, String namespace,
                   String set, Host[] hosts, String luaConfigSourcePath,
                   ClientPolicy clientPolicy, String country, String segment, String product){
        this.namespace = namespace;
        this.set = set;
        this.threadName = name;
        this.monitor = monitor;
        this.client = new AerospikeClient(clientPolicy, hosts );
        this.luaConfigSourcePath = luaConfigSourcePath;
        this.country = country;
        this.segment = segment;
        this.product = product;
    }

    @Override
    public void run()
    {
        /* Used as a filter field later in the aggregation */
        String queryFieldValue = convertCountryNameToIsoCode(country);
        queryFieldValue = queryFieldValue.concat(":").concat("SP");

        /* Calculate some new bins */
        long timeTaken = calculateProfit( country, segment, product, queryFieldValue );
        System.out.println( "Compute Profit bins for " + queryFieldValue + " in " + ( timeTaken ) + " ms.");

        /* Country filter & would-be Exp fields already built into this field queryFieldValue */
        Object [] result = getVat( Filter.contains("QF", IndexCollectionType.MAPKEYS, queryFieldValue) );
        if ( result != null ) {
            System.out.println(  "Total Vat for " + queryFieldValue + " = " + (result[0] == null ? "null" : result[0]) + " in "   + (result[1] == null ? "null" : result[1]) + " ms.");
        }
        else {
            System.out.println("No results for " + queryFieldValue);
        }

        client.close();
        monitor.notifyComplete();
    }

    private long calculateProfit(String country, String segment, String product, String queryFieldValue) {

        long startTime = System.currentTimeMillis();

        WritePolicy wPolicy = new WritePolicy();
        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
        Exp expFilter = Exp.and(
                Exp.eq(Exp.stringBin("country"), Exp.val(country)),
                Exp.eq(Exp.stringBin("segment"), Exp.val(segment)),
                Exp.eq(Exp.stringBin("product"), Exp.val(product))
        );
        wPolicy.filterExp = Exp.build(expFilter);

        Filter filter = Filter.equal("country", country);
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter(filter);

        ExecuteTask et = client.execute(wPolicy,
                stmt,
                "example",
                "totals",
                Value.get("unitsSold"),
                Value.get("mfgPrice"),
                Value.get("salesPrice"),
                Value.get( queryFieldValue )
        );
        et.waitTillComplete();
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        return timeTaken;
    }

    private Object[] getVat(Filter filter) {
        long startTime = System.currentTimeMillis();
        LuaConfig.SourceDirectory = luaConfigSourcePath;

        /*
            Note, you cannot use expression filters with queryAggregate
            QueryPolicy qPolicy = new QueryPolicy();
            qPolicy.filterExp = Exp.build(expFilter);
         */

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter( filter );
        stmt.setAggregateFunction("example", "calculateVatDue" );
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

    public static String convertCountryNameToIsoCode(String countryName) {
        for (Locale l : Locale.getAvailableLocales()) {
            if (l.getDisplayCountry().equals(countryName)) {
                return l.getISO3Country();
            }
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
