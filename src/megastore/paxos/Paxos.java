package megastore.paxos;

import megastore.paxos.message.NullMessage;
import megastore.paxos.acceptor.ListeningThread;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.proposer.PaxosProposer;

import java.util.List;

public class Paxos implements Runnable {
    private ListeningThread listeningThread;
    public PaxosAcceptor acceptor;
    public PaxosProposer proposer;
    private Object value;
    private Object finalValue; // in the end this value must be the same on all nodes
    boolean succeeded;

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
            succeeded = false;
            try {
                // Phase 1
                proposer.sendPrepareRequests();

                int nrOfAcceptors, nrOfRejectors, allParticipants;
                do {
                    Thread.sleep(10); // we wait for the proposal acceptance messages to come;
                    nrOfAcceptors = proposer.getProposalAcceptorsList().size();
                    nrOfRejectors= proposer.getProposalRejectorsNr();
                    allParticipants = proposer.getNodesURL().size();

                 //   if(nrOfRejectors>=(allParticipants+1)/2) //prod code
                      if(nrOfRejectors>0) //test code
                            return; // we will never have a majority so we return;

      //               } while(nrOfAcceptors +1 <= allParticipants/2); //production code
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
                    nrOfRejectors= proposer.getValueRejectorsNr();
                    allParticipants = proposer.getNodesURL().size();

              //      if(nrOfRejectors>=(allParticipants+1)/2) // prod code
                    if(nrOfRejectors>0)  // test code
                         return; // we will never have a majority so we return;

              //    } while(nrOfAcceptors +1 <= allParticipants/2); //production code
                } while (nrOfAcceptors + 1 < allParticipants);// my test code

                //great. Consensus was achieved on a majority of nodes.
                finalValue = proposer.getHighestPropAcc().value;
                succeeded=true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        finalValue=null;
        proposer.cleanUp();
        acceptor.cleanUp();
    }

    public boolean succeeded() {
        return succeeded;
    }
}