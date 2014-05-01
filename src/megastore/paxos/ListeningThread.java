package megastore.paxos;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ListeningThread  implements Runnable  {

    private boolean isAlive;
    private final String port;

    private Proposal highestProposalAnswered;
    private ServerSocketChannel serverSocketChannel;

    public ListeningThread(String port) {

        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(Integer.parseInt(port)));
            serverSocketChannel.configureBlocking(true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        isAlive=true;
        highestProposalAnswered=null;
        this.port=port;
    }

    @Override
    public void run() {

        try {
            while (true) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);

                ByteBuffer buffer=ByteBuffer.allocate(1024);
                int size=socketChannel.read(buffer);

                socketChannel.close();

//                while(! key.isReadable())
//                    try {
//                        Thread.sleep(10);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                buffer.flip();
                byte[] b=new byte[size];
                buffer.get(b);
                String message = new String (b);
                getResponse(message);
//                selector.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getResponse(String text) {
        if(text.startsWith("P")) {
//            If an acceptor receives a prepare request with number n greater than
//            that of any prepare request to which it has already responded,
            text=text.substring(1);
            int number= Integer.parseInt(text.split(",")[0]);
            String value= text.split(",")[2];

            if(highestProposalAnswered==null)
                return "AP";
            else if(highestProposalAnswered.nr< number )  {
               // then it responds to the request with a promise not to accept any more proposals numbered less
               // than n and with the highest-numbered proposal (if any) that it has accepted.
                String result="AP"+highestProposalAnswered.value.toString();  //wrong
                this.highestProposalAnswered=new Proposal(null, number,value);
                return result;
            }
            return "RP";
        }
        return null;
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
