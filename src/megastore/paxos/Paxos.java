package megastore.paxos;

import megastore.paxos.message.NullMessage;

import java.util.List;

public class Paxos {
    private ListeningThread listeningThread;
    public PaxosAcceptor acceptor;
    public PaxosProposer proposer;


    public Paxos(String port, List<String> nodesURL) {
        constructorExtension(nodesURL, new ListeningThread(this, port));
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

    public boolean proposeValue(Object obj) {
        try {
            // Phase 1
            proposer.sendPrepareRequests();

            int nrOfAcceptors, allParticipants;
            do {
                Thread.sleep(10); // we wait for the proposal acceptance messages to come;
                nrOfAcceptors = proposer.getProposalAcceptorsList().size();
                allParticipants = proposer.getNodesURL().size();
            } while(nrOfAcceptors +1 <= allParticipants/2);

            // Phase 2
            boolean result=proposer.isOurValueProposed(obj);
            // we save if the value for which the Paxos will achieve consensus is
            // our value or another one. Even if it's not ours, we continue because
            // we want to achieve consensus an all the nodes.

            proposer.sendAcceptRequests(obj);

            do {
                Thread.sleep(10); // we wait for the value acceptance messages to come;
                nrOfAcceptors = proposer.getValueAcceptorsList().size();
                allParticipants = proposer.getNodesURL().size();
            } while(nrOfAcceptors +1 <= allParticipants/2);

            //great. Consensus was achieved on a majority of nodes.
            return result;

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public String getCurrentUrl() {
        return listeningThread.getCurrentUrl();
    }

    public void close() {
        listeningThread.stopThread();
        new NullMessage(null,listeningThread.getCurrentUrl()).send();
    }
}