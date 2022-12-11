import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.ClientPolicy;
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

    private final String COUNTRY_SEGMENT_PRODUCT = "CSP";
    private final String COUNTRY_SEGMENT = "CS";

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

    /**
     *  queryFieldValue used as a filter field later in the aggregation
     *  we store this in a map value like FRA:SP, where FRA is the country code
     *  and SP is the selection criteria for Segment and Product.
     *
     *  It means that we have computed data and its indexed for querying. If we
     *  did not do this we would need to search for country, segment and product,
     *  where Filter might take country, but we cannot use Exp with for segment and
     *  product because we cannot use Exp with client.queryAggregate()
     */
    @Override
    public void run()
    {
        /* Calculate profit & store for country, segment and product */
        populateProfit(country, COUNTRY_SEGMENT_PRODUCT);
        query(country, COUNTRY_SEGMENT_PRODUCT, "example", "calculateVatDue", "VAT");

        /* Calculate profit & store for country, segment */
        populateProfit(country, COUNTRY_SEGMENT);
        query(country, COUNTRY_SEGMENT, "example", "calculateSales", "SALES");

        client.close();
        monitor.notifyComplete();
    }

    private void populateProfit(String country, String queryField) {
        long timeTakenCSP = calculateProfit( country, queryField );
        System.out.println( "Compute Profit bins for " + queryField + " in " + ( timeTakenCSP ) + " ms.");
    }

    private void query(String country, String queryField, String packageName, String functionName, String label){
        Object [] result = getAggregationReport( country, queryField, packageName,functionName );
        if ( result != null ) {
            System.out.println(
                    "Total " + label + " for " + queryField + " = "
                            + (result[0] == null ? "null" : result[0]) + " in "
                            + (result[1] == null ? "null" : result[1]) + " ms.");
        }
        else {
            System.out.println("No results for " + queryField);
        }
    }

    private long calculateProfit(String country, String queryField) {

        /* Calculate profit & store for country, segment and product */
        String queryFieldValue = getQueryTypeValue(country, queryField);
        Exp exp = getQueryTypeExp(queryField);

        long startTime = System.currentTimeMillis();

        WritePolicy wPolicy = new WritePolicy();
        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
        wPolicy.filterExp = Exp.build(exp);

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

    /**
     * Note, you cannot use expression filters with queryAggregate
     * QueryPolicy qPolicy = new QueryPolicy();
     * qPolicy.filterExp = Exp.build(expFilter);
     *
     * @param country
     * @param queryField
     * @return
     */
    private Object[] getAggregationReport(String country, String queryField, String packageName, String functionName ) {

        String queryFieldValue = getQueryTypeValue(country, queryField);
        Filter f = Filter.contains("QF", IndexCollectionType.MAPKEYS, queryFieldValue);

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

    public Exp getQueryTypeExp(String queryType){

        if ( queryType.equalsIgnoreCase(COUNTRY_SEGMENT_PRODUCT) )
                return  Exp.and(
                    Exp.eq(Exp.stringBin("country"), Exp.val(country)),
                    Exp.eq(Exp.stringBin("segment"), Exp.val(segment)),
                    Exp.eq(Exp.stringBin("product"), Exp.val(product))
                );
        else if (queryType.equalsIgnoreCase(COUNTRY_SEGMENT) )
                return  Exp.and(
                        Exp.eq(Exp.stringBin("country"), Exp.val(country)),
                        Exp.eq(Exp.stringBin("segment"), Exp.val(segment))
                );
        return null;
    }

    public String getQueryTypeValue(String country, String queryType){
        return convertCountryNameToIsoCode(country).concat("/").concat(queryType);
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
