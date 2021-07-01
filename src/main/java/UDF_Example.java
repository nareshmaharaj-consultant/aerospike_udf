import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;


public class UDF_Example {

    AerospikeClient client;
    String namespace = "insurance";
    String set = "expr1";

    public static void main( String args [])
    {
        UDF_Example udf = new UDF_Example();
        udf.registerLua();
        udf.insertSomeDataUDF();
        udf.insertSomeDataUDF(100000 );
        udf.insertSomeDataNonUDF( 100000 );
    }

    public UDF_Example()
    {
        AerospikeClient client = new AerospikeClient(null, "localhost", 3000);
        this.client= client;
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

    private UDF_Example registerLua() {
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

    private void insertSomeDataUDF(int number)
    {
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

    private void insertSomeDataNonUDF(int number)
    {
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
            client.put(policy, key, binType, binTemp,binMeasure);
        }

        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken non UDF is " + ( timeTaken / 1000 ) + " seconds for " + number + " docs");

    }
}
