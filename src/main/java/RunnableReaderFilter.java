import com.aerospike.client.Value;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;

import java.util.HashMap;
import java.util.Iterator;

public class RunnableReaderFilter extends  RunnableReader {

    RunnableReaderFilter(AerospikeConnectionDetails details, OperationJob job) {
        super(details, job);
    }

    @Override
    protected void query( String queryField, int reportLabel ){
        String packageName = null; String functionName = null; String label = null;
        switch(reportLabel){
            case OPERATION_REPORT_LABEL_SALES:
                packageName = "example";
                functionName = "calculateSalesFilter";
                label = "SALES";
                break;
            case OPERATION_REPORT_LABEL_VAT:
                packageName = "example";
                functionName = "calculateVatDueFilter";
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


    /**
     * Note, you cannot use expression filters with queryAggregate
     * QueryPolicy qPolicy = new QueryPolicy();
     * qPolicy.filterExp = Exp.build(expFilter);
     *
     * @param queryField
     * @return
     */
    @Override
    protected Object[] getAggregationReport(String queryField, String packageName, String functionName) {

        long startTime = System.currentTimeMillis();
        LuaConfig.SourceDirectory = luaConfigSourcePath;

        Filter f = Filter.equal("country", country);

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter( f );

        ResultSet resultSet = getAggregateQueryFilterResult( queryField, stmt, packageName, functionName );

        Iterator itr = resultSet.iterator();
        if (itr.hasNext()) {
            long endTime = System.currentTimeMillis();
            Long timeTaken = endTime - startTime;
            Object result = itr.next();
            return new Object[]{result, timeTaken};
        }
        return null;
    }


    protected ResultSet getAggregateQueryFilterResult(String queryType, Statement stmt, String packageName, String functionName){
        if ( queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT_PRODUCT) )
            return  client.queryAggregate(null, stmt, packageName, functionName,
                    Value.get(country), Value.get(segment), Value.get(product) );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY_SEGMENT) )
            return  client.queryAggregate(null, stmt, packageName, functionName,
                    Value.get(country), Value.get(segment), Value.getAsNull() );
        else if (queryType.equalsIgnoreCase(QF_COUNTRY) )
            return client.queryAggregate(null, stmt, packageName, functionName,
                    Value.get(country), Value.getAsNull(), Value.getAsNull() );;
        return null;
    }

}
