package megastore.paxos.proposer;

import megastore.DBWriteOp;
import megastore.Megastore;
import megastore.coordinator.message.InvalidateKeyMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.AcceptRequest;
import megastore.paxos.message.phase2.EnforcedAcceptRequest;
import megastore.paxos.message.phase2.InvalidateAcceptorMessage;
import megastore.paxos.message.weakerRequests.WeakerAcceptRequest;
import megastore.write_ahead_log.InvalidLogCell;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;
import systemlog.LogBuffer;

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
    private final ValidLogCell originalValue;

    private List<String> proposalAcceptorsList;
    private List<String> proposalRejectorsList;

    private List<String> valueAcceptorsList;
    private List<String> valueRejectorsList;

    private Proposal highestPropAcc;
    private int proposalNumber;
    private LogCell finalValue; // in the end this value must be the same on all nodes
    private DBWriteOp callback;
    private boolean isAnotherOneSending;
    private boolean operationHasBeenCompletedByAnotherThread;


    public PaxosProposer(long entityId, int cellNumber, Megastore megastore, List<String> nodesURL, ValidLogCell cell, DBWriteOp callback) {
        this.nodesURL = nodesURL;
        Collections.sort(this.nodesURL);
        proposalAcceptorsList=new LinkedList<String>();
        valueAcceptorsList=new LinkedList<String>();
        valueRejectorsList=new LinkedList<String>();
        proposalNumber =-1;
        highestPropAcc=null;
        proposalRejectorsList=new LinkedList<String>();
        this.megastore=megastore;
        this.entityId=entityId;
        this.cellNumber=cellNumber;
        this.originalValue=cell;
        this.callback=callback;
        isAnotherOneSending =false;
        operationHasBeenCompletedByAnotherThread=false;
    }

    public boolean proposeValueToLeader(String lastPositionsLeaderURL) {
        highestPropAcc=new Proposal(originalValue,0);
        if(megastore.getCurrentUrl().equals(lastPositionsLeaderURL)) {
            return localAcceptorAcceptsValueProposal();
        }
        else {
            new AcceptRequest(entityId, cellNumber, null, megastore.getCurrentUrl(),
                    lastPositionsLeaderURL, highestPropAcc).send();

            long start=System.currentTimeMillis();
            int responded = 0;
            try {
                do {
                    responded += valueAcceptorsList.size();
                    responded += valueRejectorsList.size();
                    if (responded == 0)
                        Thread.sleep(2); // we wait for the response to come;
                    if(System.currentTimeMillis()-start>200)
                        return false;
                } while (responded == 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return valueAcceptorsList.size() == 1;
        }
    }

    public boolean proposeValueEnforced(String olderLeaderUrl) {
        long startTime=System.currentTimeMillis();

        valueAcceptorsList.clear();
        // we can already put the leader as acceptor
        valueAcceptorsList.add(olderLeaderUrl);

        if(! olderLeaderUrl.equals(megastore.getCurrentUrl())) {
            // We also have to put on the local node and then add it to the list
            megastore.getNetworkManager().writeValueOnLog(entityId, cellNumber, originalValue);
            valueAcceptorsList.add(megastore.getCurrentUrl());
        }

        for (String url : nodesURL) {
            // the leader already accepted, so we don't have to send him again
            if (! ( megastore.getCurrentUrl().equals(url) || olderLeaderUrl.equals(url)) )
                new EnforcedAcceptRequest(entityId, cellNumber, null,
                        megastore.getCurrentUrl(), url, originalValue).send();
        }

        boolean result=waitForResult();
        if(result) {
            invalidateNonResponders();
            this.finalValue = originalValue;
            return true;
        }
        else
            return false;
    }

    public boolean waitForResult() {
        long startTime = System.currentTimeMillis();
        try {
            int nrOfAcceptors, nrOfRejectors, allParticipants;
            do {
                nrOfRejectors = valueRejectorsList.size();
                nrOfAcceptors = valueAcceptorsList.size();
                allParticipants = nodesURL.size();

                if (nrOfRejectors > ((allParticipants - 1) / 2) || System.currentTimeMillis()-startTime>200) {
                    // we will never have a majority so we return;

                    if (wasOurValueCompletedByOtherNode(originalValue))
                        return true;
                    else {
                        invalidateAcceptorsValues(valueAcceptorsList);
                        return false;
                    }
                }

                if(nrOfAcceptors <= allParticipants/2)
                    Thread.sleep(2); // we wait for the value acceptance messages to come;

            } while(nrOfAcceptors <= allParticipants/2); //production code
            // } while (nrOfAcceptors < allParticipants);// my test code
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
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
            LogBuffer.println(megastore.getCurrentUrl() + " invalidates " + url + "  for systemlog-position: " + cellNumber);
            if(!url.equals(megastore.getCurrentUrl()))
                new InvalidateKeyMessage(null, url, entityId).send();
            else
                megastore.invalidate(entityId);
        }
    }

    public boolean proposeValueTwoPhases() {
        // if the operation takes more that 1000ms we consider it false
        long startTime=System.currentTimeMillis();

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
            long sendingTime=System.currentTimeMillis();

            do {
                nrOfAcceptors = proposalAcceptorsList.size();
                nrOfRejectors= proposalRejectorsList.size();
                allParticipants = nodesURL.size();

                if(nrOfAcceptors <= allParticipants/2)
                    Thread.sleep(2); // the proposal acceptance messages didn't yet come;
                if(System.currentTimeMillis()-sendingTime>200)
                    return false;
            } while(nrOfAcceptors+nrOfRejectors < allParticipants); //production code

            if(nrOfRejectors>(allParticipants-1)/2)
                return false; // we will never have a majority so we return;

            boolean result = isOurValueProposed(originalValue);

            // if another thread is sending we have to wait to return
            while (!aquireSendingLock()) {
                Thread.sleep(2);
            }

            if(operationHasBeenCompletedByAnotherThread)
                assert true;

            if(operationHasBeenCompletedByAnotherThread &&
                    (! isOurValueProposed(originalValue)) )
                return true;


            // Phase 2
            ////////////////////
            // we save if the value for which the Paxos will achieve consensus is our value or another one.
            // Even if it's not ours, we continue because we want to achieve consensus on all the nodes.

            createAcceptProposal(originalValue);
            // ask local
            if(localAcceptorAcceptsValueProposal())
                addToValueAcceptorsList(megastore.getCurrentUrl());
            else
                addToValueRejectorsList(megastore.getCurrentUrl());

            // ask the rest
            sendAcceptRequests();

            // we can add from the start the nodes that didn't accepted the prepare request
            for(String url:proposalRejectorsList)
                valueRejectorsList.add(url);

            do {
                nrOfAcceptors = valueAcceptorsList.size();
                nrOfRejectors = valueRejectorsList.size();
                allParticipants = nodesURL.size();

                if (nrOfRejectors > ((allParticipants - 1) / 2)  ||
                        System.currentTimeMillis()-startTime > 200 ) {
                    // we will never have a majority so we return;

                    if (wasOurValueCompletedByOtherNode(originalValue)) {
                        return true;
                    }
                    else {
                        invalidateAcceptorsValues(valueAcceptorsList);
                        return false;
                    }
                }

                if(nrOfAcceptors <= allParticipants/2)
                    Thread.sleep(2); // the value acceptance messages didn't yet come;

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
        releaseFasterSendingLock();
        return false;
    }

    private boolean wasOurValueCompletedByOtherNode(ValidLogCell value) {
        try {
            while (megastore.getEntity(entityId).getLog().get(cellNumber) == null) {
                Thread.sleep(2);
            }
        } catch (Exception e) {}

        LogCell cell = megastore.getEntity(entityId).getLog().get(cellNumber);
        return value.equals(cell) &&
                        (! value.getLeaderUrl().equals(cell.getLeaderUrl()));  // different leaders
    }

    private void invalidateAcceptorsValues(List<String> valueAcceptorsList) {
        LogBuffer.println("the put operation from " + megastore.getCurrentUrl() +
                " failed, so we invalidate for position: " + cellNumber);

        synchronized (valueAcceptorsList) {
            for (String url : valueAcceptorsList)
                if (!url.equals(megastore.getCurrentUrl()))
                    new InvalidateAcceptorMessage(entityId, cellNumber, null, megastore.getCurrentUrl(), url).send();
                else {
                    Log log = megastore.getEntity(entityId).getLog();
                    if (log.get(cellNumber) != null && megastore.getCurrentUrl().equals(log.get(cellNumber).getLeaderUrl()))
                        log.append(new InvalidLogCell(), cellNumber);
                }
        }
    }

    private boolean localAcceptorAcceptsValueProposal() {
        // the code is from AcceptRequest class
        PaxosAcceptor acceptor = megastore.getListeningThread().getApropriatePaxosAcceptor(entityId, cellNumber);

        // unless it has already responded to a prepare request having a number greater than n.
        // and we didn't used that systemlog position
        if (    megastore.getEntity(entityId).isLocalOperationAccepted(cellNumber) &&
                (!megastore.getNetworkManager().isLogPosOccupied(entityId, cellNumber)) &&
                (acceptor.getHighestPropNumberAcc() <= highestPropAcc.pNumber)) {
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
        PaxosAcceptor acceptor = megastore.getListeningThread().getApropriatePaxosAcceptor(entityId, cellNumber);
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
    public void sendAcceptRequests() {
        synchronized (valueAcceptorsList) {
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

    public void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
        this.highestPropAcc.value.setLeaderUrl(megastore.getCurrentUrl());
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }

    public void addPrepareRequestAcceptor(String acceptorUrl) {
        synchronized (valueAcceptorsList) {
            proposalAcceptorsList.add(acceptorUrl);
        }
    }

    public void addPrepareRequestRejector(String nodeURL) {
        proposalRejectorsList.add(nodeURL);
    }

    public boolean isTheRightSession(String entityId, String cellNumber) {
        return (Long.parseLong(entityId) == this.entityId) &&
                (Integer.parseInt(cellNumber) == this.cellNumber);
    }

    public Megastore getMegastore() {
        return megastore;
    }

    public void addToValueAcceptorsList(String acceptorURL) {
        synchronized (valueAcceptorsList) {
            valueAcceptorsList.add(acceptorURL);
        }
    }

    public void addToValueRejectorsList(String url) {
        synchronized (valueRejectorsList) {
            valueRejectorsList.add(url);
        }
    }

    public ValidLogCell getValue() {
        return originalValue;
    }

    public DBWriteOp getCallback() {
        return callback;
    }

    public synchronized boolean aquireSendingLock() {
        if(!isAnotherOneSending) {
            isAnotherOneSending =true;
            return true;
        }
        else
            return false;
    }

    public void releaseFasterSendingLock() {
        isAnotherOneSending =false;
    }

    public void operationHasBeenCompletedByAnotherThread() {
        operationHasBeenCompletedByAnotherThread=true;
    }

    public ValidLogCell getOriginalValue() {
        return originalValue;
    }

    public boolean proposeValueWeak(String olderLeaderUrl) {
        valueAcceptorsList.clear();
        megastore.getNetworkManager().writeValueOnLog(entityId, cellNumber, originalValue);
        valueAcceptorsList.add(olderLeaderUrl);

        if(! olderLeaderUrl.equals(megastore.getCurrentUrl())) {
            // We also have to put on the local node and then add it to the list
            valueAcceptorsList.add(megastore.getCurrentUrl());
        }

        for (String url : nodesURL) {
            // the leader already accepted, so we don't have to send him again
            if (! ( megastore.getCurrentUrl().equals(url) || olderLeaderUrl.equals(url)) )
                new WeakerAcceptRequest(entityId, cellNumber, null,
                        megastore.getCurrentUrl(), url, originalValue).send();
        }
        long startTime=System.currentTimeMillis();

        boolean result=waitForResult();

        if(result) {
            invalidateNonResponders();
            this.finalValue = originalValue;
            return true;
        }
        else
            return false;
    }
}
