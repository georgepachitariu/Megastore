package megastore.network;

import megastore.Entity;
import megastore.Megastore;
import org.junit.Test;
import systemlog.LogAnalyzer;
import systemlog.SystemLog;

import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * Created by George on 10/06/2014.
 */
public class PerformanceTestTransactionNumber {

    @Test
    public void firstFunctionalTest1Entity3Servers_put_get() {

        for (int g = 10; g < 40; g=g+3) {
            System.out.println(((int)(0.05*g)+(int)(0.1*g)+(int)(0.85*g)) + "   Operations per second");

            Megastore m1 = new Megastore("61616");
            Megastore m2 = new Megastore("61617", currentIp() + ":61616");
            Megastore m3 = new Megastore("61618", currentIp() + ":61616");

            Entity e = m1.createEntity();

            int experimentDuration = 6; // in seconds

            TestDatabaseClient c1 = new TestDatabaseClient(m1, 0, (int)(0.05*g), experimentDuration);
            TestDatabaseClient c2 = new TestDatabaseClient(m2, 500, (int)(0.1*g), experimentDuration);
            TestDatabaseClient c3 = new TestDatabaseClient(m3, 1000, (int)(0.85*g), experimentDuration);

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

            LogAnalyzer analyzer = new LogAnalyzer();
            analyzer.showMedianPerfomanceTimes();
 //           analyzer.countTheMethodsUsedForEachWrite();
            analyzer.printNrOfSuccessesAndFailuresPerSecond();


            SystemLog.clear();

          //  System.out.print(SystemLog.getAsString());

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
