package megastore.paxos.proposer;

import megastore.LogBuffer;
import megastore.Megastore;
import megastore.coordinator.message.InvalidateKeyMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.AcceptRequest;
import megastore.paxos.message.phase2.EnforcedAcceptRequest;
import megastore.paxos.message.phase2.RejectAccProposalMessage;
import megastore.write_ahead_log.InvalidLogCell;
import megastore.write_ahead_log.Log;
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

    public boolean proposeValueEnforced(ValidLogCell value, String olderLeaderUrl) {
        valueAcceptorsList.clear();
        // we can already put the leader as acceptor
        valueAcceptorsList.add(olderLeaderUrl);

        if(! olderLeaderUrl.equals(megastore.getCurrentUrl())) {
            // We also have to put on the local node and then add it to the list
            megastore.getNetworkManager().writeValueOnLog(entityId, cellNumber, value);
            valueAcceptorsList.add(megastore.getCurrentUrl());
        }

        for (String url : nodesURL) {
            // the leader already accepted, so we don't have to send him again
            if (! ( megastore.getCurrentUrl().equals(url) || olderLeaderUrl.equals(url)) )
                new EnforcedAcceptRequest(entityId, cellNumber, null,
                        megastore.getCurrentUrl(), url, value).send();
        }

        try {
            int nrOfAcceptors, allParticipants;
            do {
                Thread.sleep(10); // we wait for the value acceptance messages to come;
                nrOfAcceptors = valueAcceptorsList.size();
                allParticipants = nodesURL.size();
            } while(nrOfAcceptors <= allParticipants/2); //production code
            // } while (nrOfAcceptors < allParticipants);// my test code
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        invalidateNonResponders();
        this.finalValue = value;
        return  true;
    }

    private void invalidateNonResponders() {
        //for the rest of them I have to remove them from the Coordinator
        LinkedList<String> nonResponders = new LinkedList<String>();
        for(String item: nodesURL)
            nonResponders.add(new String(item));

        nonResponders.removeAll(valueAcceptorsList);

        sendInvalidationMessages(nonResponders);
    }

    private void sendInvalidationMessages(LinkedList<String> nonResponders) {
        for(String url : nonResponders) {
            LogBuffer.println(megastore.getCurrentUrl() + " invalidates " + url + "  for log-position: " + cellNumber);
            if(!url.equals(megastore.getCurrentUrl()))
                new InvalidateKeyMessage(null, url, entityId).send();
            else
                megastore.invalidate(entityId);
        }
    }

    public boolean proposeValueTwoPhases(ValidLogCell value) {
        try {
            // Phase 1
            ///////////////////
            computeProposalNumber();

            int nrOfAcceptors, nrOfRejectors, allParticipants;

            //ask the local
            if(localAcceptorAcceptsPrepareProposal())
                proposalAcceptorsList.add(megastore.getCurrentUrl());
            else
                proposalRejectorsList.add(megastore.getCurrentUrl());

            // as the rest
            sendPrepareRequests();

            do {
                Thread.sleep(10); // we wait for the proposal acceptance messages to come;
                nrOfAcceptors = proposalAcceptorsList.size();
                nrOfRejectors= proposalRejectorsList.size();
                allParticipants = nodesURL.size();

                if(nrOfRejectors>(allParticipants-1)/2) //prod code
                    //if(nrOfRejectors>0) //test code
                    return false; // we will never have a majority so we return;

            } while(nrOfAcceptors <= allParticipants/2); //production code
            //  } while (nrOfAcceptors < allParticipants);// my test code

            // Phase 2
            ////////////////////
            boolean result = isOurValueProposed(value);
            // we save if the value for which the Paxos will achieve consensus is
            // our value or another one. Even if it's not ours, we continue because
            // we want to achieve consensus on all the nodes.

            valueAcceptorsList.clear();
            proposalRejectorsList.clear();

            createAcceptProposal(value);
            // ask local
            if(localAcceptorAcceptsValueProposal())
                valueAcceptorsList.add(megastore.getCurrentUrl());
            else
                proposalRejectorsList.add(megastore.getCurrentUrl());

            // ask the rest
            sendAcceptRequests(value);

            do {
                Thread.sleep(10); // we wait for the value acceptance messages to come;
                nrOfAcceptors = valueAcceptorsList.size();
                nrOfRejectors = proposalRejectorsList.size();
                allParticipants = nodesURL.size();

                if (nrOfRejectors > (allParticipants - 1) / 2) { //prod code
                    // if(nrOfRejectors>0)  // test code
                    invalidateAcceptorsValues(valueAcceptorsList);
                    return false; // we will never have a majority so we return;
                }

            } while(nrOfAcceptors <= allParticipants/2); //production code
            //  } while (nrOfAcceptors < allParticipants);// my test code

            //for the rest of them I have to remove them from the Coordinator
            invalidateNonResponders();

            //great. Consensus was achieved on a majority of nodes.
            this.finalValue = highestPropAcc.value;
            return result;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void invalidateAcceptorsValues(List<String> valueAcceptorsList) {
        LogBuffer.println("the put operation from " + megastore.getCurrentUrl() +
                " failed, so we invalidate for position: " + cellNumber);

        for(String url : valueAcceptorsList)
            if(! url.equals(megastore.getCurrentUrl()))
                new RejectAccProposalMessage(entityId,cellNumber,null,megastore.getCurrentUrl(),url).send();
            else {
                Log log=megastore.getEntity(entityId).getLog();
                if(log.get(cellNumber) !=null && megastore.getCurrentUrl().equals(log.get(cellNumber).getLeaderUrl()))
                    log.append(new InvalidLogCell(),cellNumber);
            }
    }

    private boolean localAcceptorAcceptsValueProposal() {
        // the code is from AcceptRequest class
        PaxosAcceptor acceptor = megastore.getThread().getApropriatePaxosAcceptor(entityId, cellNumber);


        // unless it has already responded to a prepare request having a number greater than n.
        // and we didn't used that log position
        if ( (!megastore.getNetworkManager().isLogPosOccupied(entityId, cellNumber)) &&
                //TODO make a way for him to realize that
                (acceptor.getHighestPropNumberAcc() < highestPropAcc.pNumber)) {
            acceptor.setHighestPropAcc(highestPropAcc);
            acceptor.setHighestPropNumberAcc(highestPropAcc.pNumber);
            megastore.getNetworkManager().writeValueOnLog(entityId, cellNumber, highestPropAcc.value); //we also set the final value
            return true;
        }
        else {
            return false;
        }
    }

    private boolean localAcceptorAcceptsPrepareProposal() {
        PaxosAcceptor acceptor = megastore.getThread().getApropriatePaxosAcceptor(entityId, cellNumber);
        if(acceptor.acceptsPrepareProposal(proposalNumber)) {
            Proposal newProposal = acceptor.getHighestAcceptedProposal(
                    megastore.getNetworkManager(),proposalNumber);

            if(newProposal!=null)
                this.highestPropAcc = newProposal;

            return true;
        }
        else
            return false;
    }


    // to be called by megastore after a write operation has been made local
    public void sendPrepareRequests() {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.

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
        synchronized (propAccListLock) {
            for (String url : proposalAcceptorsList)
                if(! url.equals(megastore.getCurrentUrl()))
                    new AcceptRequest(entityId, cellNumber, null, megastore.getCurrentUrl(), url, highestPropAcc).send();
        }
    }

    public void createAcceptProposal(ValidLogCell value) {
        if(highestPropAcc==null) {
            highestPropAcc=new Proposal(value, proposalNumber);
        }
        else {
            if( highestPropAcc.pNumber< proposalNumber)
                highestPropAcc.pNumber= proposalNumber;
        }
    }

    public boolean isOurValueProposed (LogCell value) {
        if (highestPropAcc==null || (! value.equals(highestPropAcc.value)) )
            return true;
        return false;
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

    public Megastore getMegastore() {
        return megastore;
    }
}
