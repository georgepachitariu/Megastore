package experiments;

import megastore.Entity;
import systemlog.OperationLogCell;
import systemlog.SystemLog;

/**
 * Created by George on 25/06/2014.
 */
public class WritingOperationThread implements Runnable {

    private final Entity entity;
    private final AutomatedDBClient client;
    private final String key;
    private final String newValue;
    private final String nodeUrl;
    private final long creationTimestamp;

    public WritingOperationThread(AutomatedDBClient client, Entity e, String key, String newValue,
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
            succeeded = entity.put(key, newValue);
            long after = System.currentTimeMillis();

            SystemLog.add(new OperationLogCell(nodeUrl, afterRead - before,
                    after - afterRead, succeeded, before, after - creationTimestamp));

        } while (!succeeded);
        client.threadCompleted(Thread.currentThread());
    }
}