package megastore.paxos.proposer;

import megastore.Megastore;
import megastore.coordinator.message.InvalidateKeyMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.AcceptRequest;
import megastore.paxos.message.phase2.EnforcedAcceptRequest;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosProposer {
    private final List<String> nodesURL;
    private final long entityId;
    private final int cellNumber;
    private final Megastore megastore;

    private Object propAccListLock=new Object();
    private List<String> proposalAcceptorsList;
    private List<String> proposalRejectorsList;

    private Object valueAccListLock=new Object();
    private List<String> valueAcceptorsList;

    private Proposal highestPropAcc;
    private int proposalNumber;
    private LogCell finalValue; // in the end this value must be the same on all nodes


    public PaxosProposer( long entityId, int cellNumber, Megastore megastore, List<String> nodesURL) {
        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
        valueAcceptorsList=new LinkedList<String>();
        proposalNumber =-1;
        highestPropAcc=null;
        proposalRejectorsList=new LinkedList<String>();
        this.megastore=megastore;
        this.entityId=entityId;
        this.cellNumber=cellNumber;
    }

    public boolean proposeValueToLeader(String lastPostionsLeaderURL, ValidLogCell cell) {
        new AcceptRequest(entityId, cellNumber, null, megastore.getCurrentUrl(),
                lastPostionsLeaderURL, new Proposal(cell, 0)).send();

        int responded=0;
        try {
            do {
                Thread.sleep(10); // we wait for the response to come;
                responded += valueAcceptorsList.size();
                responded += proposalRejectorsList.size();
            } while (responded==0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return valueAcceptorsList.size()==1;
    }

    public boolean proposeValueEnforced(ValidLogCell value) {
        valueAcceptorsList.clear();

        for (String url : nodesURL) {
            if (!megastore.getCurrentUrl().equals(url))
                new EnforcedAcceptRequest(entityId, cellNumber, null,
                        megastore.getCurrentUrl(), url, value).send();
        }

        try {
            int nrOfAcceptors, allParticipants;
            do {
                Thread.sleep(10); // we wait for the value acceptance messages to come;
                nrOfAcceptors = valueAcceptorsList.size();
                allParticipants = nodesURL.size();
                //    } while(nrOfAcceptors +1 <= allParticipants/2); //production code
            } while (nrOfAcceptors + 1 < allParticipants);// my test code
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        invalidateNonResponders();
        return  true;
    }

    public void invalidateNonResponders() {
        //for the rest of them I have to remove them from the Coordinator
        List<String> nonResponders=new LinkedList<String>();
        Collections.copy(nonResponders, nodesURL);
        nonResponders.removeAll(valueAcceptorsList);

        for(String url : nonResponders)
            new InvalidateKeyMessage(null,url,entityId).send();

        this.finalValue = highestPropAcc.value;
    }

    public boolean proposeValueTwoPhases(ValidLogCell value) {
        try {
            // Phase 1
            sendPrepareRequests();

            int nrOfAcceptors, nrOfRejectors, allParticipants;
            do {
                Thread.sleep(10); // we wait for the proposal acceptance messages to come;
                nrOfAcceptors = proposalAcceptorsList.size();
                nrOfRejectors= proposalRejectorsList.size();
                allParticipants = nodesURL.size();

                //   if(nrOfRejectors>=(allParticipants+1)/2) //prod code
                if(nrOfRejectors>0) //test code
                    return false; // we will never have a majority so we return;

                //               } while(nrOfAcceptors +1 <= allParticipants/2); //production code
            } while (nrOfAcceptors + 1 < allParticipants);// my test code

            // Phase 2
            boolean result = isOurValueProposed(value);
            // we save if the value for which the Paxos will achieve consensus is
            // our value or another one. Even if it's not ours, we continue because
            // we want to achieve consensus an all the nodes.

            sendAcceptRequests(value);

            do {
                Thread.sleep(10); // we wait for the value acceptance messages to come;
                nrOfAcceptors = valueAcceptorsList.size();
                nrOfRejectors= proposalRejectorsList.size();
                allParticipants = nodesURL.size();

                //      if(nrOfRejectors>=(allParticipants+1)/2) // prod code
                if(nrOfRejectors>0)  // test code
                    return false; // we will never have a majority so we return;

                //    } while(nrOfAcceptors +1 <= allParticipants/2); //production code
            } while (nrOfAcceptors + 1 < allParticipants);// my test code

            //for the rest of them I have to remove them from the Coordinator
            for(String url : proposalRejectorsList)
                new InvalidateKeyMessage(null,url,entityId).send();

            //great. Consensus was achieved on a majority of nodes.
            this.finalValue = highestPropAcc.value;
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }


    // to be called by megastore after a write operation
    // has been made local
    public void sendPrepareRequests() {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        computeProposalNumber();

        for (String destinationURL : nodesURL)
            if (!megastore.getCurrentUrl().equals(destinationURL)) {
                new PrepareRequest(entityId, cellNumber, null, megastore.getCurrentUrl(),
                        destinationURL,proposalNumber).send();
            }
    }

    //    Phase 2. (a) If the proposer receives a response to its prepare requests
//    (numbered n) from a majority of acceptors, then it sends an accept request to
//    each of those acceptors for a proposal numbered n with a value v, where v is the
//    value of the highest-numbered proposal among the responses, or is any value if
//    the responses reported no proposals.
    public void sendAcceptRequests(ValidLogCell value) {
        if(highestPropAcc==null) {
            highestPropAcc=new Proposal(value, proposalNumber);
        }
        else {
            if( highestPropAcc.pNumber< proposalNumber)
                highestPropAcc.pNumber= proposalNumber;
        }

        synchronized (propAccListLock) {
            for (String url : proposalAcceptorsList) {
                new AcceptRequest(entityId, cellNumber, null, megastore.getCurrentUrl(), url, highestPropAcc).send();
            }
        }
    }

    public boolean isOurValueProposed (Object value) {
        return (highestPropAcc==null);
    }

    private void computeProposalNumber() {
        int k=-1;
        for(int i=0; i<nodesURL.size(); i++)
            if(nodesURL.get(i).equals(megastore.getCurrentUrl()))
                k=i;

        // we get the biggest proposal number this node has seen
        PaxosAcceptor acceptor = megastore.getAcceptor(entityId, cellNumber);
        if(acceptor == null)
            proposalNumber = nodesURL.size() + k;
        else {
            int max =  acceptor.getHighestPropNumberAcc();
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

    public void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }

    public void addToValueAcceptorsList(String acceptorURL) {
        synchronized (valueAccListLock) {
            this.valueAcceptorsList.add(acceptorURL);
        }
    }

    public void addProposalRejector(String nodeURL) {
        boolean isAlready=false;
        for(String p : proposalRejectorsList)
            if(p.equals(nodeURL))
                isAlready=true;
        if(! isAlready)
            proposalRejectorsList.add(nodeURL);
    }

    public boolean isTheRightSession(String entityId, String cellNumber) {
        return (Long.parseLong(entityId) == this.entityId) &&
                (Integer.parseInt(cellNumber) == this.cellNumber);
    }

    public LogCell getFinalValue() {
        return finalValue;
    }
}
