package experiments;

import megastore.Entity;
import megastore.Megastore;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by George on 10/06/2014.
 */
public class AutomatedDBClient implements Runnable {
    private final Entity entity;
    private final int startingPoint;
    private final double expDurationInSec;
    private final Queue<Long> timeStampList;
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
        timeStampList =new LinkedList<Long>();
        this.opsPerSecond=opsPerSecond;
        threadList=new LinkedList<Thread>();
    }

    public void addElement(long timestamp) {
        timeStampList.add(timestamp);
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
                opsPerSecond, Thread.currentThread()),"OperationTimer").start();

        String nodeUrl = entity.getMegastore().getCurrentUrl();

        int i = 0;

        for (long start = System.currentTimeMillis();  System.currentTimeMillis() - start < 1000 * expDurationInSec; ) {
            if(! timeStampList.isEmpty()) {
                long creationTimestamp=timeStampList.poll();
                String key = String.valueOf(startingPoint + i);
                String newValue = String.valueOf(startingPoint + i);

                while(threadList.size()>=10) {
              //  while(threadList.size()>=1) {
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(timeStampList.size()+1>=2)
                    System.out.println("there are "+ (timeStampList.size()+1) + " ops in the queue");

                Thread t=new Thread(new ClientWriteOpThread(this,entity,
                        key,newValue, nodeUrl, creationTimestamp),"ClientWriteOpThr");
                t.start();
                threadList.add(t);

                i++;
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

        for(int j=0; j<i; j++) {
            String key=String.valueOf(startingPoint+j);
            String newValue=String.valueOf(startingPoint+j);

            if(entity.get(key) == null || (! newValue.equals(entity.get(key)))) {
                System.out.println("1 read Error");
                //        System.out.print(LogBuffer.getAsString());
                //      System.out.print("Error");
            }
        }
    }

    public boolean isWritingLockWeak() {
        return entity.isWritingLockWeak();
    }
}