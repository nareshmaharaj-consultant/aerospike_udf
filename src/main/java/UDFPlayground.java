import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import org.junit.Assert;

import java.util.Locale;


public class UDFPlayground {

    static AerospikeClient client;
    static String namespace = "test";
    static String set = "data";
    static int numberOfEntries = 100;

    public static void main( String args [])
    {
        Locale l = convertCountryNameToIsoCode("France");
        System.out.println( l.getISO3Country() );

//        UDFPlayground udf = null;
//        try {
//            udf = new UDFPlayground();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        udf.registerLua();
//        udf.insertSomeDataUDF(numberOfEntries );
//        udf.insertSomeDataNonUDF( numberOfEntries );
    }

    public static Locale convertCountryNameToIsoCode(String countryName) {
        for (Locale l : Locale.getAvailableLocales()) {
            if (l.getDisplayCountry().equals(countryName)) {
                return l;
            }
        }
        return null;
    }


    public UDFPlayground() throws Exception {
        AerospikeClient client = new AerospikeClient(null, "localhost", 3000);
        this.client= client;
        client.truncate(null, "test", null, null);
        registerLua();
    }

    private void useLua(Key key, int value, String type, String measure) {
        client.execute(null,
                key,
                "example",
                "validateBeforeWrite",
                Value.get("value"),
                Value.get("type"),
                Value.get("measure"),
                Value.get(value),
                Value.get(type),
                Value.get(measure)
        );
    }

    private UDFPlayground registerLua() {
        RegisterTask task = client.register(
                null,
                "/Users/nareshmaharaj/Documents/aerospike/projects/udf/src/main/java/example.lua",
                "example.lua", Language.LUA);
        // Poll cluster for completion every second for a maximum of 10 seconds.
        task.waitTillComplete(1000, 10000);
        return this;
    }

    private void insertSomeDataUDF() {
        useLua( new Key(namespace, set, 10), 22, "weather", "celcius");
        useLua( new Key(namespace, set, 12), 100, "weather", "humidity");
    }

    private void insertSomeDataUDF(int number) {
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < number; i++) {
            double r1 =  Math.ceil( Math.random() * 100 );
            double r2 = Math.ceil( Math.random() * 100 );
            int rand1 = (int)r1;
            int rand2 = (int)r2;
            useLua( new Key(namespace, set, (int)rand1), (int)rand2, "weather", "celcius");
        }

        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken UDF is " + ( timeTaken / 1000 ) + " seconds for " + number + " docs");
    }

    private void insertSomeDataNonUDF(int number) {
        long startTime = System.currentTimeMillis();

        WritePolicy policy = new WritePolicy();
        for (int i = 0; i < number; i++) {

            double r1 =  Math.ceil( Math.random() * 100 );
            double r2 = Math.ceil( Math.random() * 100 );
            int rand1 = (int)r1;
            int rand2 = (int)r2;

            Key key = new Key(namespace, set, rand1);
            Bin binType = new Bin("type", "weather");
            Bin binTemp = new Bin("value", rand2);
            Bin binMeasure = new Bin("measure", "celcius");
            Bin creationDate = new Bin("creationDate", Value.get(System.currentTimeMillis()));
            Bin updateTime = new Bin("updateTime", Value.get(System.currentTimeMillis()));
            client.put(policy, key, binType, binTemp,binMeasure,creationDate,updateTime);
        }

        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken non UDF is " + ( timeTaken / 1000 ) + " seconds for " + number + " docs");

    }

    public void createIndexOn_Measure() throws Exception {
        // Create index task
        IndexTask task = client.createIndex(null,
                namespace, // namespace
                set, // set name
                "measure_idx",
                "measure",
                IndexType.STRING
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }

    public void deleteIndexOn_Measure() throws Exception {
        // Create index task
        IndexTask task = client.dropIndex(null,
                namespace, // namespace
                set, // set name,
                "measure_idx"
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }

    public void createIndexOn_Value() throws Exception {
        // Create index task
        IndexTask task = client.createIndex(null,
                namespace, // namespace
                set, // set name
                "value_idx",
                "value",
                IndexType.NUMERIC
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }

    public void deleteIndexOn_Value() throws Exception {
        // Create index task
        IndexTask task = client.dropIndex(null,
                namespace, // namespace
                set, // set name,
                "value_idx"
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
}
