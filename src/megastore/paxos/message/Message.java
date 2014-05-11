package megastore.paxos.message;

import megastore.paxos.Paxos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by George on 01/05/2014.
 */
public abstract class Message {

    protected final String destinationURL;
    protected Paxos paxos;
    protected SocketChannel socketChannel;

    protected Message(Paxos paxos, String destinationURL) {
        this.destinationURL=destinationURL;
        this.paxos = paxos;
    }

    public void send() {
        try {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            socketChannel.connect(new InetSocketAddress(destinationURL.split(":")[0],
                    Integer.parseInt(destinationURL.split(":")[1])
            ));

            byte[] asBytes = toMessage().getBytes();
            ByteBuffer buf = ByteBuffer.allocate(asBytes.length);
            buf.clear();
            buf.put(asBytes);
            buf.flip();
            while (buf.hasRemaining()) {
                socketChannel.write(buf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }  finally {
            try {
                socketChannel.finishConnect();
                socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public abstract void act(String[] messageParts);
    public abstract String getID();
    protected abstract String toMessage();
}
