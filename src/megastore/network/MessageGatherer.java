package megastore.network;

import megastore.NetworkLatency;
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
            Thread.sleep(NetworkLatency.latency);
            /////////////////////////////////////////////

            InputStream inputStream = clientSocket.getInputStream();

            byte []all=new byte[0];
            int bytesRead=0;
            do {
                byte[] buffer=new byte[1024];
                bytesRead=inputStream.read(buffer,0,buffer.length);
                if(bytesRead!=-1)
                    all=concat(all,buffer,bytesRead);
            } while(bytesRead !=-1);

            String message = new String(all);

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

    byte[] concat(byte[] A, byte[] B, int bLength) {
        int aLen = A.length;
        int bLen = bLength;
        byte[] C= new byte[aLen+bLen];
        System.arraycopy(A, 0, C, 0, aLen);
        System.arraycopy(B, 0, C, aLen, bLen);
        return C;
    }


}
