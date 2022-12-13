import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.ExecuteTask;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;

class RunnableReader implements Runnable {
    protected final String namespace;
    protected final String set;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;
    protected String luaConfigSourcePath  = ".";
    protected String country;
    protected String segment;
    protected String product;
    private int    operation;
    private String queryField;
    private int    reportLabel;
    private long   delayBetweenJobMs;

    OperationJob job;

    public final static String QF_COUNTRY_SEGMENT_PRODUCT = "CSP";
    public final static String QF_COUNTRY_SEGMENT = "CS";
    public final static String QF_COUNTRY = "C";
    public final static String QF_BIN_NAME = "queryField";

    public final static int OPERATION_TYPE_QUERY = 1;
    public final static int OPERATION_TYPE_COMPUTE = 2;

    public final static int OPERATION_REPORT_LABEL_NONE=0;
    public final static int OPERATION_REPORT_LABEL_SALES=1;
    public final static int OPERATION_REPORT_LABEL_VAT=2;

    NumberFormat formatter = NumberFormat.getCurrencyInstance();

    public void setRunning(boolean running) {
        this.running = running;
    }

    private boolean running = true;

    RunnableReader(AerospikeConnectionDetails details, OperationJob job) {
        this.namespace = details.getNamespace();
        this.set = details.getSet();
        this.threadName = details.getName();
        this.monitor = details.getMonitor();
        this.client = new AerospikeClient( details.getClientPolicy(), details.getHosts() );
        this.luaConfigSourcePath = details.getLuaConfigSourcePath();
        this.country = job.getCountry();
        this.segment = job.getSegment();
        this.product = job.getProduct();
        this.operation = job.getOperation();
        this.queryField = job.getQueryType();
        this.reportLabel = job.getReportLabel();
        this.delayBetweenJobMs = job.getDelayBetweenJobMs();
        this.job = job;
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
                    switch (reportLabel) {
                        case OPERATION_REPORT_LABEL_SALES:
                            query( queryField, OPERATION_REPORT_LABEL_SALES );
                            break;
                        case OPERATION_REPORT_LABEL_VAT:
                            query( queryField, OPERATION_REPORT_LABEL_VAT );
                            break;
                    }
                    break;
            }
            try {
                Thread.sleep(delayBetweenJobMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.close();
        monitor.notifyComplete();
    }

    private void populateProfit(String country, String queryField) {
        long timeTakenCSP = calculateProfit( country, queryField );

        if ( job.isShowCompute() )
            System.out.println( "Compute Profit bins for "
                + additionalQueryFieldInfo(queryField) + " in " + ( timeTakenCSP ) + " ms.");
    }

    protected String additionalQueryFieldInfo(String queryField) {
        String additionalInfo = "";
        if ( queryField.equalsIgnoreCase(QF_COUNTRY) ) additionalInfo += country;
        else if (queryField.equalsIgnoreCase(QF_COUNTRY_SEGMENT)) additionalInfo += country.concat("/").concat(segment);
        else if (queryField.equalsIgnoreCase(QF_COUNTRY_SEGMENT_PRODUCT)) additionalInfo += country.concat("/").concat(segment).concat("/").concat(product);
        return "[" + queryField + "] "  + additionalInfo;
    }

    protected void query( String queryField, int reportLabel ){
        String packageName = null; String functionName = null; String label = null;
        switch(reportLabel){
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

        Object [] result = getAggregationReport( queryField, packageName, functionName );
        if ( result != null ) {

            if ( job.isShowReportResults() ) {
                HashMap map = (HashMap) result[0];

                double reportValueDouble = 0;
                double reportValueLong = 0;

                if (map.get(country) instanceof Long)
                    reportValueLong = (long) map.get(country);
                else if (map.get(country) instanceof Double)
                    reportValueDouble = (double) map.get(country);

                String amount = formatter.format(reportValueDouble > 0 ? reportValueDouble : reportValueLong);
                System.out.println(
                        "Total " + label + " for " + additionalQueryFieldInfo(queryField) + ", "
                                + (amount == null ? "null" : amount) + " in "
                                + (result[1] == null ? "null" : result[1]) + " ms.");
            }
        }
        else {
            if ( job.isShowReportNoResults() )
                System.out.println("No results for " + additionalQueryFieldInfo(queryField));
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
     * @param queryField
     * @return
     */
    protected Object[] getAggregationReport( String queryField, String packageName, String functionName ) {
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
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}