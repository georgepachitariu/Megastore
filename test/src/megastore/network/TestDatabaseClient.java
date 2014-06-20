package megastore.network;

import megastore.Entity;
import systemlog.OperationLogCell;
import megastore.Megastore;
import systemlog.SystemLog;

import java.util.Random;

/**
 * Created by George on 10/06/2014.
 */
public class TestDatabaseClient implements Runnable {
    private final Entity entity;
    private final int startingPoint;
    private final int opsPerSecond;
    private final double expDurationInSec;

    public TestDatabaseClient(Megastore megastore, int startingPoint, int opsPerSecond,
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
            String key = String.valueOf(startingPoint + i); // getRandomString(2);
            String newValue = String.valueOf(startingPoint + i);  // getRandomString(100);

            long before = System.currentTimeMillis();
            entity.get(key);
            boolean succeeded = entity.put(key, newValue);
            long after = System.currentTimeMillis();

            SystemLog.add(new OperationLogCell(nodeUrl, after - before, succeeded, before));

//        LogBuffer.println(//i+"  "+Thread.currentThread().getName() + "  logPos:  " +
            //            /* (entity.getLog().getNextPosition()-1) + "  succeded?: " +*/ succeeded + "\n\n");

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
            String key=String.valueOf(startingPoint+j); // getRandomString(2);
            String newValue=String.valueOf(startingPoint+j);  // getRandomString(100);

            if(entity.get(key) == null || (! newValue.equals(entity.get(key)))) {
                System.out.println("1 read Error");
                //        System.out.print(LogBuffer.getAsString());
                //      System.out.print("Error");
            }
        }
    }


    private static Random rand = new Random();
    private static String getRandomString(int max) {
        //return RandomStringUtils.randomAlphanumeric(max);
        String blob="";
        while(blob.length()<max) {
            int nr = Math.abs(rand.nextInt());
            blob+=String.valueOf(nr);
        }
        return blob.substring(0,max);
    }

}
