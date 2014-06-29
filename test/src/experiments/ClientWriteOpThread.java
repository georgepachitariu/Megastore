package experiments;

import megastore.DBWriteOp;
import megastore.Entity;
import systemlog.OperationLogCell;
import systemlog.SystemLog;

/**
 * Created by George on 25/06/2014.
 */
public class ClientWriteOpThread implements Runnable {

    private final Entity entity;
    private final AutomatedDBClient client;
    private final String key;
    private final String newValue;
    private final String nodeUrl;
    private final long creationTimestamp;

    public ClientWriteOpThread(AutomatedDBClient client, Entity e, String key, String newValue,
                               String nodeUrl, long creationTimestamp) {
        this.client = client;
        this.entity = e;
        this.key = key;
        this.newValue = newValue;
        this.nodeUrl = nodeUrl;
        this.creationTimestamp = creationTimestamp;
    }

    @Override
    public void run() {
        boolean succeeded;
        do {
            client.acquireWritingLock();

            long before = System.currentTimeMillis();
            entity.get(key);
            long afterRead = System.currentTimeMillis();
            succeeded = new DBWriteOp(entity,key,newValue).execute();
            long after = System.currentTimeMillis();

            SystemLog.add(new OperationLogCell(nodeUrl, newValue, afterRead - before,
                    after - afterRead, succeeded, before, after - creationTimestamp));

        } while (!succeeded);
        client.threadCompleted(Thread.currentThread());
    }
}