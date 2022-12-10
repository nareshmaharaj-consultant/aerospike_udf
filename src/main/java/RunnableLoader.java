import com.aerospike.client.*;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
    AtomicInteger numberofRecordsWritten;

    RunnableLoader(String name,
                   int numOfRecords,
                   int startKey,
                   Monitor monitor,
                   String namespace,
                   String set,
                   Host[] hosts,
                   SalesData[] salesDataSeed700,
                   ClientPolicy clientPolicy,
                   AtomicInteger numberofRecordsWritten ) {

        threadName = name;
        this.client = new AerospikeClient(clientPolicy, hosts );
        this.numberOfDocuments = numOfRecords;
        this.startKey = startKey;
        this.monitor = monitor;
        this.namespace = namespace;
        this.set = set;
        this.salesData = salesDataSeed700;
        this.numberofRecordsWritten = numberofRecordsWritten;
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
            numberofRecordsWritten.incrementAndGet();

            if (i % UDFExampleDataLoader.batchWriteLump == 0 ){
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