package experiments;

import megastore.Entity;
import megastore.Megastore;
import megastore.Write;

import java.util.LinkedList;

/**
 * Created by George on 10/06/2014.
 */
public class AutomatedDBClient implements Runnable {
    private final Entity entity;
    private final int startingPoint;
    private final double expDurationInSec;
    private final LinkedList<Write> writesQueue;
    private final double opsPerSecond;
    private LinkedList<Thread> threadList;

    public AutomatedDBClient(Megastore megastore, int startingPoint, double opsPerSecond,
                             double expDurationInSec) {
        try {
            while(megastore.getEntity(0)==null) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.entity= megastore.getEntity(0);
        this.startingPoint=startingPoint;
        this.expDurationInSec = expDurationInSec;
        writesQueue =new LinkedList<Write>();
        this.opsPerSecond=opsPerSecond;
        threadList=new LinkedList<Thread>();
    }

    public void addOperation(LinkedList<Write> newWrites) {
        synchronized (writesQueue) {
            writesQueue.addAll(newWrites);
        }
    }

    public synchronized void threadCompleted(Thread t) {
        threadList.remove(t);
    }

    public synchronized void  acquireWritingLock() {
        while(! entity.isReadyForNextWriteOperation()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        entity.blockNextOperation();
    }

    @Override
    public void run() {
        new Thread(new OperationTimerThread(this,
                opsPerSecond, Thread.currentThread(), startingPoint),"OperationTimer").start();

        String nodeUrl = entity.getMegastore().getCurrentUrl();

        int i = 0;

        for (long start = System.currentTimeMillis();  System.currentTimeMillis() - start < 1000 * expDurationInSec; ) {
            if(! writesQueue.isEmpty()) {
                i=Integer.parseInt( writesQueue.getLast().key );

                int limit;
//                if(!Optimisations.Optim)
//                    limit=25;  // activation point: parallel writes - we don't use this optimisation anymore
//                else
                    limit=1;


                while(threadList.size()>=limit) {         // activation point: parallel writes
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                LinkedList<Write> list;
                synchronized (writesQueue) {
                    list=new LinkedList<Write>();
                    list.add(writesQueue.poll());  // we will always have one

//                    if(Optimisations.Optim)
//                        while(! writesQueue.isEmpty()) {  // but we can put all of them
//                            list.add(writesQueue.poll());     // OPTIMISATION: buffering writes (activation point)
//                        }                                                       //
                }

                Thread t=new Thread(new ClientWriteOpThread(this,entity,
                        list, nodeUrl),"ClientWriteOpThr");
                t.start();
                threadList.add(t);
            }
            else
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }

        while(threadList.size()>0) {
            try {
                threadList.get(0).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


//        for(int j=i; j>=startingPoint; j--) {
//            String key=String.valueOf(j);
//            String newValue=String.valueOf(j);
//
//            if(entity.get(key) == null || (! newValue.equals(entity.get(key))))
//                System.out.println("Write error on log");
//        }
    }

    public boolean isWritingLockWeak() {
        return entity.isWritingLockWeak();
    }
}
