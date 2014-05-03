package megastore.paxos;

import megastore.paxos.message.NullMessage;

import java.util.List;

public class Paxos {
    private List<String> nodesURL;

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

    public String getCurrentUrl() {
        return listeningThread.getCurrentUrl();
    }

    public void close() {
        listeningThread.stopThread();
        new NullMessage(null,listeningThread.getCurrentUrl()).send();
    }
}