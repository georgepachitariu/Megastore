package megastore.paxos;

import megastore.paxos.message.phase1.PrepareRequest;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosProposer {
    private final List<String> nodesURL;
    private final Paxos paxos;
    private int highestAcceptedNumber;
    private List<String> proposalAcceptorsList;
    private Proposal highestPropAcc;


    public PaxosProposer(Paxos paxos, List<String> nodesURL) {
        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
        this.paxos=paxos;
        highestAcceptedNumber =-1;
    }

    // to be called by megastore after a write operation
    // has been made local
    public void sendPrepareRequests(Object value) {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        for (String destinationURL : nodesURL)
            if (!paxos.getCurrentUrl().equals(destinationURL)) {
                new PrepareRequest(paxos, paxos.getCurrentUrl(),
                        destinationURL,getProposalNumber()).send();
            }
    }

    //    Phase 2. (a) If the proposer receives a response to its prepare requests
//            (numbered n) from a majority of acceptors, then it sends an accept request to
//    each of those acceptors for a proposal numbered n with a value v, where v is the
//    value of the highest-numbered proposal among the responses, or is any value if
//    the responses reported no proposals.
    public void sendAcceptRequests(Object value) {

    }

    private int getProposalNumber() {
        int k=-1;
        for(int i=0; i<nodesURL.size(); i++)
            if(nodesURL.get(i).equals(paxos.getCurrentUrl()))
                k=i;

        if(highestAcceptedNumber == -1)
            return nodesURL.size() + k;

        for( ; k<= highestAcceptedNumber; k+=nodesURL.size());
        return k;
    }

    public void addNodeAsAcceptorOfProposal(String acceptorUrl) {
        proposalAcceptorsList.add(acceptorUrl);
    }

    public List<String> getProposalAcceptorsList() {
        return proposalAcceptorsList;
    }

    public void setHighestAcceptedNumber(int highestAcceptedNumber) {
        this.highestAcceptedNumber = highestAcceptedNumber;
    }

    public int getHighestAcceptedNumber() {
        return highestAcceptedNumber;
    }

    public void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }
}
