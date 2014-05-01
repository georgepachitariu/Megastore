package paxos;

import megastore.paxos.ListeningThread;
import megastore.paxos.Paxos;
import org.junit.Test;

import java.util.LinkedList;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class Phase1Unit {
//    Phase 1. (a) A proposer selects a proposal number n and sends a prepare
//    request with number n to a majority of acceptors.
//            (b) If an acceptor receives a prepare request with number n greater than
//    that of any prepare request to which it has already responded, then it responds
//    to the request with a promise not to accept any more proposals numbered less
//    than n and with the highest-numbered proposal (if any) that it has accepted.

    @Test
    public void sendPrepareRequest() throws InterruptedException {
        //    Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        //    request with number n to a majority of acceptors.
        String n2="192.168.1.100:61617";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n2);

        Paxos p1=new Paxos("61616",list);

        ListeningThread mockedThread = spy(new ListeningThread("61617"));

       Thread runThread = new Thread(mockedThread);
        runThread .setDaemon(false);
        runThread .start();

        p1.proposeValue(new Integer(7));

        Thread.sleep(100);
        verify(mockedThread).getResponse("P1,192.168.1.100:61616,7");
        mockedThread.stopThread();
    }




}
