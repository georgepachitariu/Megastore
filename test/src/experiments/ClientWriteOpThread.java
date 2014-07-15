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
    private final AutomatedDBClient entityWorker;
    private final String key;
    private final String newValue;
    private final String nodeUrl;
    private final long creationTimestamp;

    public ClientWriteOpThread(AutomatedDBClient entityWorker, Entity e, String key, String newValue,
                               String nodeUrl, long creationTimestamp) {
        this.entityWorker = entityWorker;
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
            entityWorker.acquireWritingLock();

            long before = System.currentTimeMillis();
            entity.get(key);
            long afterRead = System.currentTimeMillis();
            boolean isWritingLockWeak=entityWorker.isWritingLockWeak();
            succeeded = new DBWriteOp(entity,key,newValue,isWritingLockWeak).execute();
            long after = System.currentTimeMillis();

            SystemLog.add(new OperationLogCell(nodeUrl, newValue, afterRead - before,
                    after - afterRead, succeeded, before, after - creationTimestamp));

        } while (!succeeded);
        entityWorker.threadCompleted(Thread.currentThread());
    }
}