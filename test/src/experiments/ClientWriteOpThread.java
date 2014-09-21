package experiments;

import megastore.DBWriteOp;
import megastore.Entity;
import megastore.Write;
import systemlog.OperationLogCell;
import systemlog.SystemLog;

import java.util.List;

/**
 * Created by George on 25/06/2014.
 */
public class ClientWriteOpThread implements Runnable {

    private final Entity entity;
    private final AutomatedDBClient entityWorker;
    private final List<Write> writes;
    private final String nodeUrl;

    public ClientWriteOpThread(AutomatedDBClient entityWorker, Entity e, List<Write> writes,
                               String nodeUrl) {
        this.entityWorker = entityWorker;
        this.entity = e;
        this.writes = writes;
        this.nodeUrl = nodeUrl;
    }

    @Override
    public void run() {
        boolean succeeded;
        do {
            entityWorker.acquireWritingLock();

            long before = System.currentTimeMillis();
            entity.get(writes.get(0).key);
            long afterRead = System.currentTimeMillis();
            boolean isWritingLockWeak=entityWorker.isWritingLockWeak();
            succeeded = new DBWriteOp(entity,writes,isWritingLockWeak).execute();
            long after = System.currentTimeMillis();

            for(Write wr : writes) {
                SystemLog.add(new OperationLogCell(nodeUrl, wr.newValue, afterRead - before,
                        after - afterRead, succeeded, before, after - wr.creationTimeStamp, entity.getMegastore().getCurrentUrl()));
            }
        } while (!succeeded);
        entityWorker.threadCompleted(Thread.currentThread());
    }
}