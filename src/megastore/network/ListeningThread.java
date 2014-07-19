package megastore.network;

import megastore.coordinator.message.InvalidateKeyMessage;
import megastore.network.message.AvailableNodesMessage;
import megastore.network.message.IntroductionMessage;
import megastore.network.message.NetworkMessage;
import megastore.network.message.NewEntityMessage;
import megastore.network.message.paxos_optimisation.AreYouUpToDateMessage;
import megastore.network.message.paxos_optimisation.LogCellsRequestedMessage;
import megastore.network.message.paxos_optimisation.RequestValidLogCellsMessage;
import megastore.network.message.paxos_optimisation.UpToDateConfirmedMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.message.NullMessage;
import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.paxos.message.phase1.PrepReqAccepted;
import megastore.paxos.message.phase1.PrepReqAcceptedWithProp;
import megastore.paxos.message.phase1.PrepReqRejected;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.*;
import megastore.paxos.message.weakerRequests.WeakerAR_Accepted;
import megastore.paxos.message.weakerRequests.WeakerAR_Rejected;
import megastore.paxos.message.weakerRequests.WeakerAcceptRequest;
import megastore.paxos.proposer.PaxosProposer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class ListeningThread  implements Runnable  {

    private NetworkManager networkManager;
    private boolean isAlive;
    private final String port;

    private List<PaxosProposer> proposingSessionsOpen;
    private List<PaxosAcceptor> acceptingSessionsOpen;

    private List<NetworkMessage> knownNetworkMessages;
    private List<PaxosProposerMessage> knownProposerMessages;
    private List<PaxosAcceptorMessage> knownAcceptorMessages;

    private ServerSocket serverSocket;
    private LinkedList<Thread> workersList;

    public ListeningThread(NetworkManager networkManager, String port) {
        proposingSessionsOpen=new LinkedList<PaxosProposer>();
        acceptingSessionsOpen=new LinkedList<PaxosAcceptor>();
        this.networkManager=networkManager;
        workersList=new LinkedList<Thread>();

        try {
            serverSocket = new ServerSocket(Integer.parseInt(port));
        } catch (IOException e) {
            e.printStackTrace();
        }

        isAlive=true;
        this.port=port;

        addNetworkMessages();
        addPaxosProposerMessages();
        addPaxosAcceptorMessages();
    }

    private void addPaxosAcceptorMessages() {
        knownAcceptorMessages = new LinkedList<PaxosAcceptorMessage>();
        knownAcceptorMessages.add(new PrepReqAccepted(-1,-1,null,null,null));
        knownAcceptorMessages.add(new PrepReqAcceptedWithProp(-1,-1,null,null,null,null));
        knownAcceptorMessages.add(new PrepReqRejected(-1,-1,null,null,null,-1));
        knownAcceptorMessages.add(new AR_Accepted(-1,-1,null,null,null,-1));
        knownAcceptorMessages.add(new AR_Rejected(-1,-1,null,null,null,-1));
        knownAcceptorMessages.add(new EnforcedAR_Accepted(-1,-1,null,null,null));
        knownAcceptorMessages.add(new EnforcedAR_Rejected(-1,-1,null,null,null));
        knownAcceptorMessages.add(new WeakerAR_Accepted(-1,-1,null,null,null));
        knownAcceptorMessages.add(new WeakerAR_Rejected(-1,-1,null,null,null));
    }

    private void addPaxosProposerMessages() {
        knownProposerMessages =new LinkedList<PaxosProposerMessage>();
        knownProposerMessages.add(new PrepareRequest(-1, -1, networkManager, null, null, -1));
        knownProposerMessages.add(new AcceptRequest(-1, -1, networkManager, null, null, null));
        knownProposerMessages.add(new InvalidateAcceptorMessage(-1,-1,networkManager,null,null));
        knownProposerMessages.add(new EnforcedAcceptRequest(-1, -1, networkManager, null, null, null));
        knownProposerMessages.add(new WeakerAcceptRequest(-1, -1, networkManager, null, null, null));
    }

    private void addNetworkMessages() {
        knownNetworkMessages =new LinkedList<NetworkMessage>();
        knownNetworkMessages.add(new IntroductionMessage(networkManager,null,null));
        knownNetworkMessages.add(new AvailableNodesMessage(networkManager,null,null));
        knownNetworkMessages.add(new NewEntityMessage(networkManager,null,-1,null));
        knownNetworkMessages.add(new InvalidateKeyMessage(networkManager,null,-1));
        knownNetworkMessages.add(new AreYouUpToDateMessage(-1,networkManager,null,null));
        knownNetworkMessages.add(new UpToDateConfirmedMessage(-1,networkManager,null,null));
        knownNetworkMessages.add(new RequestValidLogCellsMessage(-1,networkManager,null,null,null,-1));
        knownNetworkMessages.add(new LogCellsRequestedMessage(-1,networkManager,null,null));
        knownNetworkMessages.add(new NullMessage(null));
    }

    public void addProposer(PaxosProposer paxosProposer) {
        synchronized (proposingSessionsOpen) {
            proposingSessionsOpen.add(paxosProposer);
        }
    }

    @Override
    public void run() {
        while (isAlive) {
            Socket clientSocket=null;
            try {
                clientSocket = serverSocket.accept();
           //     SystemLog.add(new systemlog.NetworkMessage(getCurrentUrl()));

                Thread worker=new Thread(new MessageGatherer(clientSocket, this),"MessageReaderFromNetwork");
                workersList.add(worker);
                worker.start();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for(Thread t : workersList) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean treatMessage(String[] parts) {
        for(NetworkMessage m : knownNetworkMessages) {
            if (m.getID().equals(parts[0])) {
                m.act(parts);
                return true;
            }
        }

        long entityId=Long.parseLong(parts[0]);
        int cellNumber=Integer.parseInt(parts[1]);
        PaxosAcceptor session = getApropriatePaxosAcceptor(entityId,cellNumber);
        for(PaxosProposerMessage m : knownProposerMessages) {
            if (m.getID().equals(parts[2]) && session!=null) {
                m.setAcceptor(session);
                m.act(parts);
                return true;
            }
        }

        PaxosProposer proposer= getApropriatePaxosProposer(parts);
        for(PaxosAcceptorMessage m : knownAcceptorMessages) {
            if (m.getID().equals(parts[2])) {
                m.setProposer(proposer);
                if(proposer!=null) {
                    //sometimes a majority has been achieved (of acceptors or rejectors)
                    // before some messages (like this one) have arrived
                    // so the proposer doesn't exist anymore
                    m.act(parts);
                }
                return true;
            }
        }
        return false;
    }

    public PaxosProposer getApropriatePaxosProposer(String[] parts) {
        PaxosProposer session=null;
        synchronized (proposingSessionsOpen) {
            for (PaxosProposer p : proposingSessionsOpen)
                if (p.isTheRightSession(parts[0], parts[1]))
                    session = p;
        }
        return session;
    }

    public PaxosAcceptor getApropriatePaxosAcceptor(long entityId, int cellNumber) {
        PaxosAcceptor session=null;

        synchronized (acceptingSessionsOpen) {
            for (PaxosAcceptor p : acceptingSessionsOpen)
                if (p.isTheRightSession(entityId, cellNumber))
                    session = p;
        }
        if(session==null) {
            session = new PaxosAcceptor(entityId,cellNumber);
        }
        return session;
    }

    public void stopThread() {
        isAlive = false;
        new NullMessage(getCurrentUrl()).send();
    }

    public String getCurrentUrl() {
        try {
            return InetAddress.getLocalHost().getHostAddress()+ ":" +port;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    public  List<PaxosAcceptor> getAcceptorSession() {
        return acceptingSessionsOpen;
    }

    public void removeProposer(PaxosProposer proposer) {
        synchronized (proposingSessionsOpen) {
            proposingSessionsOpen.remove(proposer);
        }
    }

    public PaxosProposer getProposer(long entityId, int cellNumber) {
        PaxosProposer session=null;
        synchronized (proposingSessionsOpen) {
            for (PaxosProposer p : proposingSessionsOpen)
                if (p.isTheRightSession(Long.toString(entityId), Integer.toString(cellNumber)))
                    session = p;
        }
        return session;
    }
}
