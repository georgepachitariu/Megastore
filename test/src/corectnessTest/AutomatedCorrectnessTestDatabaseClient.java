package corectnessTest;

import megastore.Entity;
import megastore.Megastore;
import systemlog.OperationLogCell;
import systemlog.SystemLog;

public class AutomatedCorrectnessTestDatabaseClient implements Runnable {
    private final Entity entity;
    private final int startingPoint;
    private final int opsPerSecond;
    private final double expDurationInSec;

    public AutomatedCorrectnessTestDatabaseClient(Megastore megastore, int startingPoint, int opsPerSecond,
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
        this.opsPerSecond=opsPerSecond;
        this.expDurationInSec = expDurationInSec;
    }

    @Override
    public void run() {
        if(opsPerSecond==0)
            return;
        int timeToWait= 1000/ opsPerSecond;
        String nodeUrl = entity.getMegastore().getCurrentUrl();

        int i = 0;
        for (long start = System.currentTimeMillis();  System.currentTimeMillis() - start < 1000 * expDurationInSec; ) {
            String key = String.valueOf(startingPoint + i);
            String newValue = String.valueOf(startingPoint + i);

            long before = System.currentTimeMillis();
            entity.get(key);
            long afterRead = System.currentTimeMillis();
            boolean succeeded = entity.put(key, newValue);
            long after = System.currentTimeMillis();

            SystemLog.add(new OperationLogCell(nodeUrl, afterRead - before,
                    after - afterRead, succeeded, before, after - 0));

            timeToWait-=(after - before);
            if(timeToWait<0)
                timeToWait=0;
            if (succeeded) {
                i++;
                try {
                    Thread.sleep(timeToWait);
                    timeToWait = 1000/ opsPerSecond;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        for(int j=0; j<i; j++) {
            String key=String.valueOf(startingPoint+j);
            String newValue=String.valueOf(startingPoint+j);

            if(entity.get(key) == null || (! newValue.equals(entity.get(key)))) {
                System.out.println("1 read Error");
            }
        }

        System.out.println(i + " operations succeded on node " + nodeUrl);
    }

}
