package megastore;

import java.util.Random;

/**
 * Created by George on 11/08/2014.
 */
public class NetworkLatency {
    static public int latency=0;
    static final Random r=new Random();

    public static void  randomize() {
        latency=Math.abs(r.nextInt())%3;
    }
}
