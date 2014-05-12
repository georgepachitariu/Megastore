package megastore.network;

import megastore.network.message.AvailableNodesMessage;
import megastore.network.message.IntroductionMessage;
import megastore.network.message.NetworkMessage;
import megastore.network.message.NewEntityMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.paxos.message.phase1.PrepReqAccepted;
import megastore.paxos.message.phase1.PrepReqAcceptedWithProp;
import megastore.paxos.message.phase1.PrepReqRejected;
import megastore.paxos.message.phase1.PrepareRequest;
import megastore.paxos.message.phase2.AR_Accepted;
import megastore.paxos.message.phase2.AR_Rejected;
import megastore.paxos.message.phase2.AcceptRequest;
import megastore.paxos.proposer.PaxosProposer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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

    private ServerSocketChannel serverSocketChannel;

    public ListeningThread(NetworkManager networkManager, String port) {
        proposingSessionsOpen=new LinkedList<PaxosProposer>();
        acceptingSessionsOpen=new LinkedList<PaxosAcceptor>();
        this.networkManager=networkManager;

        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(Integer.parseInt(port)));
            serverSocketChannel.configureBlocking(true);
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
    }

    private void addPaxosProposerMessages() {
        knownProposerMessages =new LinkedList<PaxosProposerMessage>();
        knownProposerMessages.add(new PrepareRequest(-1,-1,networkManager,null,null,-1));
        knownProposerMessages.add(new AcceptRequest(-1,-1,networkManager,null,null,null));
    }

    private void addNetworkMessages() {
        knownNetworkMessages =new LinkedList<NetworkMessage>();
        knownNetworkMessages.add(new IntroductionMessage(networkManager,null,null));
        knownNetworkMessages.add(new AvailableNodesMessage(networkManager,null,null));
        knownNetworkMessages.add(new NewEntityMessage(networkManager,null,-1,null));
    }

    public void addProposer(PaxosProposer paxosProposer) {
        proposingSessionsOpen.add(paxosProposer);
    }

    @Override
    public void run() {
        SocketChannel socketChannel=null;

        while (isAlive) {
            try {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);

                ByteBuffer buffer = ByteBuffer.allocate(1024);
                int size = socketChannel.read(buffer);
                if (size <= 0)
                    continue;

                buffer.flip();
                byte[] b = new byte[size];
                buffer.get(b);
                String message = new String(b);

                String[] parts = message.split(",");

                boolean recognized = treatMessage(parts);

                if (recognized == false)
                    System.out.println("Network Message Not Recognized");

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socketChannel.finishConnect();
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean treatMessage(String[] parts) {
        boolean recognized=false;

        for(NetworkMessage m : knownNetworkMessages) {
            if (m.getID().equals(parts[0])) {
                m.act(parts);
                return true;
            }
        }

        PaxosAcceptor session = getApropriatePaxosAcceptor(parts);
        for(PaxosProposerMessage m : knownProposerMessages) {
            if (m.getID().equals(parts[2])) {
                m.setAcceptor(session);
                m.act(parts);
                return true;
            }
        }

        PaxosProposer proposer= getApropriatePaxosProposer(parts);
        for(PaxosAcceptorMessage m : knownAcceptorMessages) {
            if (m.getID().equals(parts[2])) {
                m.setProposer(proposer);
                m.act(parts);
                return true;
            }
        }
        return false;
    }

    private PaxosProposer getApropriatePaxosProposer(String[] parts) {
        PaxosProposer session=null;
        for(PaxosProposer p : proposingSessionsOpen)
            if(p.isTheRightSession(parts[0],parts[1]))
                session= p;

        return session;
    }

    private PaxosAcceptor getApropriatePaxosAcceptor(String[] parts) {
        PaxosAcceptor session=null;
        long p1=Long.parseLong(parts[0]);
        int p2=Integer.parseInt(parts[1]);

        for(PaxosAcceptor p : acceptingSessionsOpen)
            if(p.isTheRightSession(p1,p2))
                session= p;
        if(session==null) {
            session = new PaxosAcceptor(p1,p2);
        }
        return session;
    }

    public void stopThread() {
        isAlive = false;
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
}
