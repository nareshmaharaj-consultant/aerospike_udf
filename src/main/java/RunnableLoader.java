import com.aerospike.client.*;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.BatchPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class RunnableLoader implements Runnable {

    private final String namespace;
    private final String set;
    private SalesData[] salesData = null;
    private int numberOfDocuments;
    private int startKey;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;

    RunnableLoader(String name,
                   int numOfRecords,
                   int startKey,
                   Monitor monitor,
                   String namespace,
                   String set,
                   Host[] hosts,
                   SalesData[] salesDataSeed700
    ) {
        threadName = name;
        this.client = new AerospikeClient(null, hosts );
        this.numberOfDocuments = numOfRecords;
        this.startKey = startKey;
        this.monitor = monitor;
        this.namespace = namespace;
        this.set = set;
        this.salesData = salesDataSeed700;
    }

    @Override
    public void run()
    {
        BatchPolicy bPolicy = new BatchPolicy(client.batchPolicyDefault);
        bPolicy.setTimeout(1000);
        List<BatchRecord> records = new ArrayList<>();
        Random random = new Random();

        for (int i = startKey; i < (numberOfDocuments + startKey); i++) {
            Key key     = new Key(namespace, set, i);
            int next    = random.nextInt(salesData.length);
            Operation[] operations = salesData[ next ].getOperationRandomiseData();
            records.add(new BatchWrite(key,operations));

            if (i % UDFExampleRandomDataLoaderSalesLines.batchWriteLump == 0 ){
                client.operate(bPolicy,records);
                records.clear();
            }
        }
        client.operate(bPolicy,records);
        client.close();
        monitor.notifyComplete();
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}