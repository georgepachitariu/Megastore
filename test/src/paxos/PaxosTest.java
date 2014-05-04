package paxos;

import megastore.paxos.Paxos;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Random;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosTest {

    @Test
    public void stressTestPaxos() throws InterruptedException {
        String url1="192.168.1.100:61616";
        String url2="192.168.1.100:61617";
        String url3="192.168.1.100:61618";
        LinkedList<String> list =new LinkedList<String>();
        list.add(url1);
        list.add(url2);
        list.add(url3);

        Paxos p1=new Paxos("61616", list,"1");
        Paxos p2=new Paxos("61617", list,"2");
        Paxos p3=new Paxos("61618", list,"3");

        Random rand = new Random();
        for(int i=0; i<100; i++) {

            int n1 = rand.nextInt(1000);        p1.setValue(String.valueOf(n1));
            int n2 = rand.nextInt(1000);        p2.setValue(String.valueOf(n2));
            int n3 = rand.nextInt(1000);        p3.setValue(String.valueOf(n3));

            Thread thr1 = new Thread(p1, "thr1");
            Thread thr2 = new Thread(p2, "thr2");
            Thread thr3 = new Thread(p3, "thr3");

            thr1.start();
            thr2.start();
            thr3.start();

            thr1.join();    thr2.join();    thr3.join();

            Assert.assertTrue(p1.getFinalValue().equals(p2.getFinalValue()) &&
                    p2.getFinalValue().equals(p3.getFinalValue()));

            int victoriousThread=0;
            if(p1.getFinalValue().equals(String.valueOf(n1)))       victoriousThread=1;
            if(p2.getFinalValue().equals(String.valueOf(n2)))       victoriousThread=2;
            if(p3.getFinalValue().equals(String.valueOf(n3)))       victoriousThread=3;

            System.out.println(p1.getFinalValue() + " . And the thread who won is -- " + victoriousThread);

            p1.cleanUp();
            p2.cleanUp();
            p3.cleanUp();
        }

        p1.close();
        p2.close();
        p3.close();
    }
}
