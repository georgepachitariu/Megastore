package megastore.network;

import systemlog.LogBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * Created by George on 24/06/2014.
 */
public class MessageGatherer implements Runnable {

    private final Socket clientSocket;
    private final ListeningThread listeningThread;

    public MessageGatherer(Socket clientSocket, ListeningThread listeningThread) {
        this.clientSocket = clientSocket;
        this.listeningThread=listeningThread;
    }

    @Override
    public void run() {
        try {

            ////////////////////////////////////////////////
            Thread.sleep(30);
            /////////////////////////////////////////////

            InputStream inputStream = clientSocket.getInputStream();
            while (inputStream.available() == 0) {
                Thread.sleep(1);
            }
            byte[] buffer = new byte[inputStream.available()];

            inputStream.read(buffer);
            clientSocket.getInputStream().close();

            String message = new String(buffer);

            String[] parts = message.split(",");
            boolean recognized = listeningThread.treatMessage(parts);

            if (!recognized)
                LogBuffer.println("Network Message Not Recognized");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientSocket != null)
                    clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




}
