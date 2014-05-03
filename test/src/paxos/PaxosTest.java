package paxos;

import megastore.paxos.Paxos;
import org.junit.Test;

import java.util.LinkedList;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosTest {

    @Test
    public void sendingAProposalAndGetAllResponses() throws InterruptedException {
        String n1="192.168.1.100:61616";
        String n2="192.168.1.100:61617";
        String n3="192.168.1.100:61618";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n1);
        list.add(n2);
        list.add(n3);

        Paxos p1=new megastore.paxos.Paxos("61616", list);
        Paxos p2=new megastore.paxos.Paxos("61617", list);
        Paxos p3=new megastore.paxos.Paxos("61618", list);

        p3.proposeValue("1");
        p1.proposeValue("2");
        p2.proposeValue("3");
        p3.proposeValue("4");
        p1.proposeValue("5");

        p1.close();
        p2.close();
        p3.close();
    }
}
