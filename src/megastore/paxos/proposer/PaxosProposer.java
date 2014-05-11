package megastore.paxos.proposer;

import megastore.paxos.Paxos;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.AcceptRequest;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosProposer {
    private final Paxos paxos;
    private final List<String> nodesURL;

    private Object propAccListLock=new Object();
    private List<String> proposalAcceptorsList;
    private int proposalRejectorsNr;

    private Object valueAccListLock=new Object();
    private List<String> valueAcceptorsList;

    private Proposal highestPropAcc;
    private int proposalNumber;
    private int valueRejectorsNr;


    public PaxosProposer(Paxos paxos, List<String> nodesURL) {
        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
        valueAcceptorsList=new LinkedList<String>();
        this.paxos=paxos;
        proposalNumber =-1;
        highestPropAcc=null;
        proposalRejectorsNr=0;
        valueRejectorsNr=0;
    }

    // to be called by megastore after a write operation
    // has been made local
    public void sendPrepareRequests() {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        computeProposalNumber();

        for (String destinationURL : nodesURL)
            if (!paxos.getCurrentUrl().equals(destinationURL)) {
                new PrepareRequest(paxos, paxos.getCurrentUrl(),
                        destinationURL,proposalNumber).send();
            }
    }

    //    Phase 2. (a) If the proposer receives a response to its prepare requests
//    (numbered n) from a majority of acceptors, then it sends an accept request to
//    each of those acceptors for a proposal numbered n with a value v, where v is the
//    value of the highest-numbered proposal among the responses, or is any value if
//    the responses reported no proposals.
    public void sendAcceptRequests(Object value) {
        //      boolean operationResult;
        if(highestPropAcc==null) {
            highestPropAcc=new Proposal(value, proposalNumber);
        }
        else {
           if( highestPropAcc.pNumber< proposalNumber)
               highestPropAcc.pNumber= proposalNumber;
//            operationResult=false;
            // because even if we succeed, we don't insert the expected value
            // but another one
        }

        synchronized (propAccListLock) {
            for (String url : proposalAcceptorsList) {
                new AcceptRequest(paxos, paxos.getCurrentUrl(), url, highestPropAcc).send();
            }
        }
    }

    public boolean isOurValueProposed (Object value) {
        return (highestPropAcc==null);
    }

    private void computeProposalNumber() {
        int k=-1;
        for(int i=0; i<nodesURL.size(); i++)
            if(nodesURL.get(i).equals(paxos.getCurrentUrl()))
                k=i;

        // we get the biggest proposal number this node has seen
        int max =  paxos.acceptor.getHighestPropNumberAcc();
        if(max == -1)
            proposalNumber = nodesURL.size() + k;
        else {
            for( ; k<= max; k+=nodesURL.size());
            proposalNumber=k;

        }
    }

    public void addNodeAsAcceptorOfProposal(String acceptorUrl) {
        if(! proposalAcceptorsList.contains(acceptorUrl)) {
            synchronized (propAccListLock) {
                proposalAcceptorsList.add(acceptorUrl);
            }
        }
    }

    public List<String> getProposalAcceptorsList() {
        synchronized (propAccListLock) {
            return proposalAcceptorsList;
        }
    }

    public int getProposalNumber() {
        return proposalNumber;
    }

    public void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }

    public List<String> getValueAcceptorsList() {
        synchronized (valueAccListLock) {
            return valueAcceptorsList;
        }
    }

    public void addToValueAcceptorsList(String acceptorURL) {
        synchronized (valueAccListLock) {
            this.valueAcceptorsList.add(acceptorURL);
        }
    }

    public List<String> getNodesURL() {
        return nodesURL;
    }

    public int getProposalRejectorsNr() {
        return proposalRejectorsNr;
    }

    public void increaseProposalRejectorsNr() {
        proposalRejectorsNr++;
    }

    public void cleanUp() {
        proposalAcceptorsList=new LinkedList<String>();
        valueAcceptorsList=new LinkedList<String>();
        highestPropAcc=null;
        proposalNumber =-1;
        proposalRejectorsNr=0;
        valueRejectorsNr=0;
    }

    public void cleanProposalAcceptorsList() {
        proposalAcceptorsList=new LinkedList<String>();
        valueAcceptorsList=new LinkedList<String>();
    }

    public void increaseValueRejectorsNr() {
        valueRejectorsNr++;
    }

    public int getValueRejectorsNr() {
        return valueRejectorsNr;
    }
}
