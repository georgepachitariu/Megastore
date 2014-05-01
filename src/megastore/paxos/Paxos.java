package megastore.paxos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class Paxos {
    private List<String> nodesURL;
    private ListeningThread listeningThread;

    public Paxos(String port, List<String> nodesURL) {
        // a listeningThread that is listening for proposals or accept proposals
        listeningThread =new ListeningThread(port);
        Thread runThread = new Thread(this.listeningThread);
        runThread .setDaemon(false);
        runThread .start();

        this.nodesURL = nodesURL;
    }

    public Paxos(String port, List<String> nodesURL, ListeningThread thread) {
        Thread runThread  = new Thread(thread);
        runThread .setDaemon(false);
        runThread .start();

        this.listeningThread = thread;
        this.nodesURL = nodesURL;
    }

    // to be called by megastore after a write operation
    // has been made local
    public void proposeValue(Object value) {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        for(String s : nodesURL)
            if(! listeningThread.getCurrentUrl().equals( s ))
                sendMessage(s,
                        new Proposal(listeningThread.getCurrentUrl(),  1,   value). toString()
                );
    }

    public void sendMessage(String nodeUrl, String message) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            socketChannel.connect(new InetSocketAddress(nodeUrl.split(":")[0],
                    Integer.parseInt(nodeUrl.split(":")[1])
            ));


            ByteBuffer buf = ByteBuffer.allocate(1000);
            buf.clear();
            buf.put(message.getBytes());

            buf.flip();

            while (buf.hasRemaining()) {
                socketChannel.write(buf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}