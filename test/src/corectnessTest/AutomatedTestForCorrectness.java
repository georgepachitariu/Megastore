package corectnessTest;

import megastore.Entity;
import megastore.Megastore;
import megastore.write_ahead_log.Log;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;


public class AutomatedTestForCorrectness {
    // we run multiple sessions of the database
    // and each time at the end we check that each value inserted (successfully) is found in the log
    // and the logs for each node contain the same values in the same order

    @Test
    public void test() {

        for (int g = 10; g < 300; g=g+10) {
            Megastore m1 = new Megastore("61616");
            Megastore m2 = new Megastore("61617", currentIp() + ":61616");
            Megastore m3 = new Megastore("61618", currentIp() + ":61616");

            Entity e = m1.createEntity();

            int experimentDuration = 20; // in seconds

            AutomatedCorrectnessTestDatabaseClient c1 = new AutomatedCorrectnessTestDatabaseClient(m1, 0, (int)(0.05*g), experimentDuration);
            AutomatedCorrectnessTestDatabaseClient c2 = new AutomatedCorrectnessTestDatabaseClient(m2, 500, (int)(0.1*g), experimentDuration);
            AutomatedCorrectnessTestDatabaseClient c3 = new AutomatedCorrectnessTestDatabaseClient(m3, 1000, (int)(0.85*g), experimentDuration);

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

            //now we assert that the systemlog positions show the same thing on all nodes
            Log l1 = m1.getEntity(0).getLog();
            Log l2 = m2.getEntity(0).getLog();
            Log l3 = m3.getEntity(0).getLog();

            for(int i=0; i<l1.size; i++) {
                boolean correct=true;
                if(l1.get(i) == null)
                    if(! (l2.get(i)==null && l3.get(i)==null))
                        correct=false;

                if(!(l1.get(i).toString().equals(l2.get(i).toString())))
                    correct=false;
                if(!(l2.get(i).toString().equals(l3.get(i).toString())))
                    correct=false;
                if(!correct)
                    System.out.println("1 logcell without consensus");
            }

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
