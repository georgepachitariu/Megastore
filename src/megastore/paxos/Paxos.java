package megastore.paxos;

import megastore.paxos.message.NullMessage;

import java.util.List;

public class Paxos implements Runnable {
    private ListeningThread listeningThread;
    public PaxosAcceptor acceptor;
    public PaxosProposer proposer;
    private Object value;
    private Object finalValue; // in the end this value must be the same on all nodes

    private boolean acceptRequestsDidNotSucceded;
    private boolean prepareRequestsDidNotSucceed;

    public Paxos(String port, List<String> nodesURL, Object obj) {
        constructorExtension(nodesURL, new ListeningThread(this, port));
        this.value=obj;
        finalValue=null;
    }

    public Paxos(List<String> nodesURL, ListeningThread thread) {
        constructorExtension(nodesURL, thread);
    }

    private void constructorExtension(List<String> nodesURL, ListeningThread thread) {
        // a listeningThread that is listening for proposals or accept proposals
        this.listeningThread = thread;
        Thread runThread = new Thread(thread);
        runThread.setDaemon(false);
        runThread.start();

        this.acceptor =new PaxosAcceptor();
        this.proposer =new PaxosProposer(this, nodesURL);
    }

    @Override
    public void run() {
        do {
            acceptRequestsDidNotSucceded = false;
            prepareRequestsDidNotSucceed = false;
            try {
                // Phase 1
                proposer.sendPrepareRequests();

                int nrOfAcceptors, allParticipants;
                do {
                    Thread.sleep(10); // we wait for the proposal acceptance messages to come;
                    nrOfAcceptors = proposer.getProposalAcceptorsList().size();
                    allParticipants = proposer.getNodesURL().size();
                    if(prepareRequestsDidNotSucceed)
                        break;   // we break from this round and start again
                    //    } while(nrOfAcceptors +1 <= allParticipants/2); //production code
                } while (nrOfAcceptors + 1 < allParticipants);// my test code

                // Phase 2
                boolean result = proposer.isOurValueProposed(value);
                // we save if the value for which the Paxos will achieve consensus is
                // our value or another one. Even if it's not ours, we continue because
                // we want to achieve consensus an all the nodes.

                proposer.sendAcceptRequests(value);

                do {
                    Thread.sleep(10); // we wait for the value acceptance messages to come;
                    nrOfAcceptors = proposer.getValueAcceptorsList().size();
                    allParticipants = proposer.getNodesURL().size();
                    if(acceptRequestsDidNotSucceded ||
                            prepareRequestsDidNotSucceed)
                        break;   // we break from this round and start again
                    //   } while(nrOfAcceptors +1 <= allParticipants/2); //production code
                } while (nrOfAcceptors + 1 < allParticipants);// my test code

                //great. Consensus was achieved on a majority of nodes.
                finalValue = proposer.getHighestPropAcc().value;

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while(acceptRequestsDidNotSucceded ||
                        prepareRequestsDidNotSucceed);
    }

    public String getCurrentUrl() {
        return listeningThread.getCurrentUrl();
    }

    public void close() {
        listeningThread.stopThread();
        new NullMessage(null,listeningThread.getCurrentUrl()).send();
    }

    public void setFinalValue(Object value) {
        this.finalValue = value;
    }

    public Object getFinalValue() {
        return finalValue;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void cleanUp() {
        value=null;
        proposer.cleanUp();
        acceptor.cleanUp();
    }

    public void acceptRequestsDidNotSucceded() {
        acceptRequestsDidNotSucceded = true;
    }
    public void prepareRequestsDidNotSucceded() {
        prepareRequestsDidNotSucceed = true;
    }
}