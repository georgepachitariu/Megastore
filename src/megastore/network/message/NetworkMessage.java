package megastore.network.message;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by George on 01/05/2014.
 */
public abstract class NetworkMessage implements Runnable {

    protected String destinationIP;
    private  int destinationPort;
    public String sourceUrl;

    public NetworkMessage(String destinationURL) {
        if(destinationURL!=null) {
            String[] parts = destinationURL.split(":");
            this.destinationIP = parts[0];
            if(parts.length!=2)
                System.out.println("");
            this.destinationPort = Integer.parseInt(parts[1]);
        }
    }

    @Override
    public void run() {
        if(destinationIP==null)
             return;


        Socket socket=null;
        try {
            socket= new Socket(destinationIP, destinationPort);
            socket.setSoLinger(false,0);

            byte[] messageAsBytes = toMessage().getBytes();
            socket.getOutputStream().write(messageAsBytes);
            socket.getOutputStream().flush();
        } catch (IOException e) {
            e.printStackTrace();
        }  finally {
            try {
                if(socket!=null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void send() {
        Thread t = new Thread(this);
        t.start();
    }

    public abstract void act(String[] messageParts);
    public abstract String getID();
    protected abstract String toMessage();
}
