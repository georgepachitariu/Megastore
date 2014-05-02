package megastore.paxos;

import megastore.paxos.message.*;

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

    private Paxos paxos;
    private boolean isAlive;
    private final String port;
    private List<Message> knownMessageTypes;

    private ServerSocketChannel serverSocketChannel;

    public ListeningThread(Paxos paxos,String port) {

        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(Integer.parseInt(port)));
            serverSocketChannel.configureBlocking(true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.paxos=paxos;
        isAlive=true;
        this.port=port;

        knownMessageTypes=new LinkedList();
        knownMessageTypes.add(new PrepareRequest(paxos,null,null,0,0,null));
        knownMessageTypes.add(new PrepReqAccepted(paxos,null,null,0, null));
        knownMessageTypes.add(new PrepReqRejected(null,null,null,0));
        knownMessageTypes.add(new NullMessage(null,null));
    }

    public ListeningThread(Paxos paxos,String port, List<Message> knownMessageTypes) {
        this(paxos,port);
        this.knownMessageTypes=knownMessageTypes;
    }

    @Override
    public void run() {
        try {
            while (isAlive) {
                SocketChannel socketChannel= serverSocketChannel.accept();
                socketChannel.configureBlocking(true);

                ByteBuffer buffer=ByteBuffer.allocate(1024);
                int size=socketChannel.read(buffer);

                socketChannel.close();

                buffer.flip();
                byte[] b=new byte[size];
                buffer.get(b);
                String message = new String (b);

                String[] parts = message.split(",");

                boolean recognized=false;
                for(Message m : knownMessageTypes)
                    if(m.getID().equals(parts[0])) {
                        m.act(parts);
                        recognized=true;
                    }
                if(recognized==false)
                    System.out.println("Network Message Not Recognized");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
}
