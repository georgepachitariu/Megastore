package megastore.network;

import megastore.Entity;
import org.junit.Assert;

import java.util.Random;

/**
 * Created by George on 10/06/2014.
 */
public class DatabaseClient implements Runnable {
    private final Entity entity;
    private final double startingPoint;

    public DatabaseClient(Entity entity, int startingPoint) {
        this.entity=entity;
        this.startingPoint=startingPoint;
    }

    @Override
    public void run() {
        int iterations=3;
        for(int i=0; i<iterations; i++) {
            String key=String.valueOf(startingPoint + i); // getRandomString(2);
            String newValue=String.valueOf(startingPoint+i);  // getRandomString(100);

            entity.get(key);
            boolean succeeded=entity.put(key,newValue);
            System.out.println(//i+"  "+Thread.currentThread().getName() + "  logPos:  " +
                   /* (entity.getLog().getNextPosition()-1) + "  succeded?: " +*/ succeeded + "\n\n");

            if(!succeeded)
                i--;
        }

        for(int i=0; i<iterations; i++) {
            String key=String.valueOf(startingPoint+i); // getRandomString(2);
            String newValue=String.valueOf(startingPoint+i);  // getRandomString(100);

            Assert.assertTrue(newValue.equals(entity.get(key)));
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
