package megastore.network.message;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by George on 01/05/2014.
 */
public abstract class NetworkMessage {

    protected String destinationIP;
    private  int destinationPort;

    public NetworkMessage(String destinationURL) {
        if(destinationURL!=null) {
            String[] parts = destinationURL.split(":");
            this.destinationIP = parts[0];
            this.destinationPort = Integer.parseInt(parts[1]);
        }
    }

    public void send() {
        Socket socket=null;
        try {
            socket= new Socket(destinationIP, destinationPort);
            socket.setSoLinger(false,0);

            byte[] messageAsBytes = toMessage().getBytes();
            socket.getOutputStream().write(messageAsBytes);
            socket.getOutputStream().flush();
            socket.getOutputStream().close();
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

    public abstract void act(String[] messageParts);
    public abstract String getID();
    protected abstract String toMessage();
}
