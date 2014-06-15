package megastore.network;

import megastore.Entity;
import megastore.LogBuffer;
import megastore.Megastore;
import megastore.write_ahead_log.Log;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * Created by George on 10/06/2014.
 */
public class PerformanceTestTransactionNumber {

    @Test
    public void firstFunctionalTest1Entity3Servers_put_get() {

        for(int g=0; g< 100; g++) {
            System.out.println("Testing round: "+g);

            Megastore m1 = new Megastore("61616");
            Megastore m2 = new Megastore("61617", currentIp() + ":61616");
            Megastore m3 = new Megastore("61618", currentIp() + ":61616");

            Entity e = m1.createEntity();

            DatabaseClient c1 = new DatabaseClient(m1, 0);
            DatabaseClient c2 = new DatabaseClient(m2, 500);
            DatabaseClient c3 = new DatabaseClient(m3, 1000);

            Thread t1 = new Thread(c1, "Client 1");
            Thread t2 = new Thread(c2, "Client 2");
            Thread t3 = new Thread(c3, "Client 3");

            t1.start();
            t2.start();
            t3.start();

            try {
                t1.join();
                t2.join();
                t3.join();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            //finally we read from each replica (to synchronize with catchup)
            m1.getEntity(0).get("0");
            m2.getEntity(0).get("0");
            m3.getEntity(0).get("0");

            //now we assert that the log positions show the same thing on all nodes
            Log l1 = m1.getEntity(e.getEntityID()).getLog();
            Log l2 = m2.getEntity(e.getEntityID()).getLog();
            Log l3 = m3.getEntity(e.getEntityID()).getLog();

            for(int i=0; i<l1.size; i++) {
                if((! l1.get(i).toString().equals(l2.get(i).toString()) ) &&
                        (! l2.get(i).toString().equals(l3.get(i).toString()) )  ) {
                 //   System.out.print(LogBuffer.getAsString());
                    System.out.println("1 Error at log");
//                    LogBuffer.println("");
                }
            }

            LogBuffer.clear();

            m1.close();
            m2.close();
            m3.close();
        }
    }

    static String currentIp() {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }
}
