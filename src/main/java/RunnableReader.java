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
    private int    operation;
    private String queryField;
    private int    reportlabel;

    public final static String QF_COUNTRY_SEGMENT_PRODUCT = "CSP";
    public final static String QF_COUNTRY_SEGMENT = "CS";
    public final static String QF_COUNTRY = "C";
    public final static String QF_BIN_NAME = "queryField";

    public final static int OPERATION_TYPE_QUERY = 1;
    public final static int OPERATION_TYPE_COMPUTE = 2;

    public final static int OPERATION_REPORT_LABEL_NONE=0;
    public final static int OPERATION_REPORT_LABEL_SALES=1;
    public final static int OPERATION_REPORT_LABEL_VAT=2;

    public void setRunning(boolean running) {
        this.running = running;
    }

    private boolean running = true;

    RunnableReader(String name, Monitor monitor, String namespace,
                   String set, Host[] hosts, String luaConfigSourcePath,
                   ClientPolicy clientPolicy, String country, String segment,
                   String product, int operationType, String queryField,
                   int reportLabel) {
        this.namespace = namespace;
        this.set = set;
        this.threadName = name;
        this.monitor = monitor;
        this.client = new AerospikeClient(clientPolicy, hosts );
        this.luaConfigSourcePath = luaConfigSourcePath;
        this.country = country;
        this.segment = segment;
        this.product = product;
        this.operation = operationType;
        this.queryField = queryField;
        this.reportlabel = reportLabel;

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
        while (running){
            switch( operation ){
                case OPERATION_TYPE_COMPUTE:
                    populateProfit(country, queryField);
                    break;
                case OPERATION_TYPE_QUERY:
                    switch (reportlabel) {
                        case OPERATION_REPORT_LABEL_SALES:
                            query( country, queryField, OPERATION_REPORT_LABEL_SALES );
                            break;
                        case OPERATION_REPORT_LABEL_VAT:
                            query( country, queryField, OPERATION_REPORT_LABEL_VAT );
                            break;
                    }
                    break;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.close();
        monitor.notifyComplete();
    }

    private void populateProfit(String country, String queryField) {
        long timeTakenCSP = calculateProfit( country, queryField );
        System.out.println( "Compute Profit bins for " + queryField + " in " + ( timeTakenCSP ) + " ms.");
    }

    private void query(String country, String queryField, int reportLabel ){
        String packageName = null; String functionName = null; String label = null;
        switch(reportlabel){
            case OPERATION_REPORT_LABEL_SALES:
                packageName = "example";
                functionName = "calculateSales";
                label = "SALES";
                break;
            case OPERATION_REPORT_LABEL_VAT:
                packageName = "example";
                functionName = "calculateVatDue";
                label = "VAT";
                break;
        }

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
                Value.get("salesPrice")
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

    public Exp getQueryTypeExp(String queryType){

        if ( queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT_PRODUCT) )
                return  Exp.and(
                    Exp.eq(Exp.stringBin("country"), Exp.val(country)),
                    Exp.eq(Exp.stringBin("segment"), Exp.val(segment)),
                    Exp.eq(Exp.stringBin("product"), Exp.val(product))
                );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT) )
                return  Exp.and(
                        Exp.eq(Exp.stringBin("country"), Exp.val(country)),
                        Exp.eq(Exp.stringBin("segment"), Exp.val(segment))
                );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY) )
            return Exp.eq(Exp.stringBin("country"), Exp.val(country));
        return null;
    }

    public String getQueryFieldMapValue(String queryType){
        if ( queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT_PRODUCT) )
            return  country.concat("/").concat(segment).concat("/").concat(product);
        else if (queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT) )
            return  country.concat("/").concat(segment);
        else if (queryType.equalsIgnoreCase(QF_COUNTRY) )
            return country;
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
