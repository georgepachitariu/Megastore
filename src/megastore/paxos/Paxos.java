package megastore.paxos;

import megastore.paxos.message.NullMessage;
import megastore.paxos.message.PrepareRequest;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Paxos {
    private List<String> nodesURL;
    private ListeningThread listeningThread;
    private PrepareRequest highestPrepReqAnswered;
    private List<String> proposalAcceptorsList;

    public Paxos(String port, List<String> nodesURL) {
        // a listeningThread that is listening for proposals or accept proposals
        listeningThread = new ListeningThread(this, port);
        Thread runThread = new Thread(this.listeningThread);
        runThread.setDaemon(false);
        runThread.start();

        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
    }

    public Paxos(List<String> nodesURL, ListeningThread thread) {
        Thread runThread = new Thread(thread);
        runThread.setDaemon(false);
        runThread.start();

        this.listeningThread = thread;
        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
    }

    // to be called by megastore after a write operation
    // has been made local
    public void sendPrepareRequests(Object value) {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        for (String destinationURL : nodesURL)
            if (!listeningThread.getCurrentUrl().equals(destinationURL)) {
                new PrepareRequest(this, listeningThread.getCurrentUrl(), destinationURL,
                        getPaxosRound(value.toString()), getProposalNumber(), value).send();
            }
    }

//    Phase 2. (a) If the proposer receives a response to its prepare requests
//            (numbered n) from a majority of acceptors, then it sends an accept request to
//    each of those acceptors for a proposal numbered n with a value v, where v is the
//    value of the highest-numbered proposal among the responses, or is any value if
//    the responses reported no proposals.
    public void sendAcceptRequests(Object value) {

    }

    private int getPaxosRound(String objValue) {
        PrepareRequest prop = highestPrepReqAnswered;
        if(prop==null)
            return 1;
        return prop.paxosRound + 1;
    }

    private int getProposalNumber() {
        int k=-1;
        for(int i=0; i<nodesURL.size(); i++)
            if(nodesURL.get(i).equals(listeningThread.getCurrentUrl()))
                k=i;

        if(highestPrepReqAnswered ==null)
            return nodesURL.size() + k;
        int prevRoundNr= highestPrepReqAnswered.paxosRound;
        return (prevRoundNr + 1) * nodesURL.size() +k;
    }

    public void addNodeAsAcceptorOfProposal(String acceptorUrl) {
        proposalAcceptorsList.add(acceptorUrl);
    }

    public void setHighestPrepReqAnswered(PrepareRequest highestPrepReqAnswered) {
        this.highestPrepReqAnswered = highestPrepReqAnswered;
    }

    public PrepareRequest getHighestPrepReqAnswered() {
        return highestPrepReqAnswered;
    }

    public String getCurrentUrl() {
        return listeningThread.getCurrentUrl();
    }

    public List<String> getProposalAcceptorsList() {
        return proposalAcceptorsList;
    }

    public void setProposalNumber(int number) {
        highestPrepReqAnswered.proposalNumber=number;
    }

    public void close() {
        listeningThread.stopThread();
        new NullMessage(null,listeningThread.getCurrentUrl()).send();
    }
}