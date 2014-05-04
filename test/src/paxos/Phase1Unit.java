/*
package paxos;

import megastore.paxos.ListeningThread;
import megastore.paxos.Paxos;
import megastore.paxos.message.phase1.PrepareRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

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
        String n1="192.168.1.100:61616";
        String n2="192.168.1.100:61617";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n1);
        list.add(n2);

        Paxos p1=new Paxos("61616",list);

        List knownMessageTypes=new LinkedList();
        PrepareRequest mockedPrepareRequest = mock(PrepareRequest.class);
        when(mockedPrepareRequest.getID()).thenCallRealMethod();
        knownMessageTypes.add(mockedPrepareRequest);

        ListeningThread listeningThread = new ListeningThread(null, "61617", knownMessageTypes);
        Thread runThread = new Thread(listeningThread);
        runThread .setDaemon(false);
        runThread .start();

        p1.proposer.sendPrepareRequests();

        Thread.sleep(100);
        verify(mockedPrepareRequest).act("PrepareRequest,192.168.1.100:61616,2".split(","));
        listeningThread.stopThread();
        p1.close();
    }

//        (b) If an acceptor receives a prepare request with number n greater than
//    that of any prepare request to which it has already responded, then it responds
//    to the request with a promise not to accept any more proposals numbered less
//    than n and with the highest-numbered proposal (if any) that it has accepted.

    @Test
    public void send_respondPrepareRequest_withoutPreviousProposal() throws InterruptedException {
        String n1="192.168.1.100:61616";
        String n2="192.168.1.100:61617";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n1);
        list.add(n2);

        Paxos p1=new Paxos("61616", list);
        Paxos p2=new Paxos("61617", list);

        p1.proposer.sendPrepareRequests();

        Thread.sleep(100);
        List<String> result = p1.proposer.getProposalAcceptorsList();
        Assert.assertTrue(result.size() == 1 && result.get(0).equals("192.168.1.100:61617"));
        p1.close();
        p2.close();
    }

    @Test
    public void sendPrepareRequest_withPreviousProposal() throws InterruptedException {
        String n1="192.168.1.100:61616";
        String n2="192.168.1.100:61617";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n1);
        list.add(n2);

        Paxos p1=new Paxos("61616", list);
        Paxos p2=new Paxos("61617", list);

        p1.proposer.sendPrepareRequests();

        while(p1.proposer.getHighestAcceptedNumber() == -1)
            Thread.sleep(100);

        p1.proposer.sendPrepareRequests();

        while(p1.proposer.getHighestAcceptedNumber()<3)
            Thread.sleep(100);

        Assert.assertTrue(p2.acceptor.getHighestPropNumberAcc()==4);

        p1.close();
        p2.close();
    }

    @Test
    public void sendingAProposalAndGetAllResponses() throws InterruptedException {
        String n1="192.168.1.100:61616";
        String n2="192.168.1.100:61617";
        String n3="192.168.1.100:61618";
        LinkedList<String> list =new LinkedList<String>();
        list.add(n1);
        list.add(n2);
        list.add(n3);

        Paxos p1=new Paxos("61616", list);
        Paxos p2=new Paxos("61617", list);
        Paxos p3=new Paxos("61618", list);

        p3.proposer.sendPrepareRequests();

        Thread.sleep(100);
        Assert.assertTrue(p3.proposer.getProposalAcceptorsList().size() == 2);

        p1.close();
        p2.close();
        p3.close();
    }


}
*/
