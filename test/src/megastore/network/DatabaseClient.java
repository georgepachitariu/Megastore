package megastore.network;

import megastore.Entity;
import megastore.LogBuffer;
import megastore.Megastore;

import java.util.Random;

/**
 * Created by George on 10/06/2014.
 */
public class DatabaseClient implements Runnable {
    private final Entity entity;
    private final double startingPoint;

    public DatabaseClient(Megastore megastore, int startingPoint) {
        try {
            while(megastore.getEntity(0)==null) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.entity= megastore.getEntity(0);
        this.startingPoint=startingPoint;
    }

    @Override
    public void run() {


        int iterations=10;
        for(int i=0; i<iterations; i++) {
            String key=String.valueOf(startingPoint + i); // getRandomString(2);
            String newValue=String.valueOf(startingPoint+i);  // getRandomString(100);

            entity.get(key);
            boolean succeeded=entity.put(key,newValue);
            LogBuffer.println(//i+"  "+Thread.currentThread().getName() + "  logPos:  " +
                   /* (entity.getLog().getNextPosition()-1) + "  succeded?: " +*/ succeeded + "\n\n");

            if(!succeeded)
                i--;
        }

        for(int i=0; i<iterations; i++) {
            String key=String.valueOf(startingPoint+i); // getRandomString(2);
            String newValue=String.valueOf(startingPoint+i);  // getRandomString(100);

            if(entity.get(key) == null || (! newValue.equals(entity.get(key)))) {
                System.out.println("1 read Error");
             //  System.out.print(LogBuffer.getAsString());
              //  System.out.print("Error");
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
