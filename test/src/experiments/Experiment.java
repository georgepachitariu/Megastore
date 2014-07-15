package experiments;

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
public class Experiment {

    @Test
    public void experiment1() {

        for (int g = 18; g < 42; g=g+2) {
            System.out.println(((0.05*g)+(0.1*g)+(0.85*g)) + "   Operations per second");

            Megastore m1 = new Megastore("61616");
            Megastore m2 = new Megastore("61617", currentIp() + ":61616");
            Megastore m3 = new Megastore("61618", currentIp() + ":61616");

            Entity e = m1.createEntity();
            int experimentDuration = 20; // in seconds
//
//            AutomatedDBClient c1 = new AutomatedDBClient(m1, 0, (g), experimentDuration);
//            AutomatedDBClient c2 = new AutomatedDBClient(m2, 500, (g), experimentDuration);
//            AutomatedDBClient c3 = new AutomatedDBClient(m3, 1000, (g), experimentDuration);

            AutomatedDBClient c1 = new AutomatedDBClient(m1, 0, (0.05*g), experimentDuration);
            AutomatedDBClient c2 = new AutomatedDBClient(m2, 500, (0.1*g), experimentDuration);
            AutomatedDBClient c3 = new AutomatedDBClient(m3, 1000, (0.85*g), experimentDuration);

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
            analyzer.removeFirst3SecondsFromDividedLog();

           // analyzer.showMedianPerfomanceTimes();
            analyzer.showMedianWaitingTimes();
           // analyzer.countTheMethodsUsedForEachWrite();
          //  analyzer.printNrOfSuccessesAndFailuresPerSecond();
           // analyzer.printInfoAboutOperations();
            SystemLog.clear();

            System.out.print("\n\n");

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
