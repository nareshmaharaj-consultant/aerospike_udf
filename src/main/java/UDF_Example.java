import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;

public class UDF_Example {

    AerospikeClient client;
    String namespace = "insurance";
    String set = "expr1";

    public static void main( String args [])
    {
        new UDF_Example().registerLua().insertSomeData();
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
                "/Users/nareshmaharaj/Documents/aerospike/projects/expressions/src/main/java/example.lua",
                "example.lua", Language.LUA);
        // Poll cluster for completion every second for a maximum of 10 seconds.
        task.waitTillComplete(1000, 10000);
        return this;
    }

    private void insertSomeData() {
//        WritePolicy policy = new WritePolicy();
//        Key key = new Key(namespace, set, 10);
//        Bin binType = new Bin("type", "weather");
//        Bin binTemp = new Bin("value", 22);
//        Bin binMeasure = new Bin("measure", "celcius");
//        client.put(policy, key, binType, binTemp,binMeasure);

        useLua( new Key(namespace, set, 10), 22, "weather", "celcius");
        useLua( new Key(namespace, set, 12), 100, "weather", "humidity");
    }
}
