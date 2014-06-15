package megastore.network;

import megastore.Megastore;
import megastore.network.message.IntroductionMessage;
import megastore.write_ahead_log.LogCell;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 11/05/2014.
 */
public class NetworkManager {
    private Megastore megastore;
    private ListeningThread listeningThread;
    private final Thread runThread;
    private List<String> nodesURL;

    public NetworkManager(Megastore megastore, String port) {
        this.megastore=megastore;
        listeningThread = new ListeningThread(this, port);
        runThread = new Thread(listeningThread);
        runThread.setDaemon(false);
        runThread.start();

        nodesURL=new LinkedList<String>();
        nodesURL.add(listeningThread.getCurrentUrl());
    }

    public void updateNodesFrom(String nodeUrl) {
        new IntroductionMessage(this, listeningThread.getCurrentUrl(), nodeUrl).send();
        while(nodesURL.size() == 1l) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void putNodes (String blob) {
        nodesURL= new LinkedList<String>();
        String[] parts= blob.split("!");
        for(String p : parts) {
            nodesURL.add(p);
        }
    }

    public String getAvailableNodesAsString() {
        String blob="";
        for(String str: nodesURL)
            blob+=str + "!";
        return blob;
    }

    public void close() {
        listeningThread.stopThread();
        try {
            runThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void addNode(String source) {
        this.nodesURL.add(source);
    }

    public List<String> getNodesURL() {
        return nodesURL;
    }

    public ListeningThread getListeningThread() {
        return listeningThread;
    }

    public String getCurrentUrl() {
        return listeningThread.getCurrentUrl();
    }

    public void writeValueOnLog(long entityId, int cellNumber, LogCell value) {
        megastore.append(entityId, cellNumber, value);
    }

    public Megastore getMegastore() {
        return megastore;
    }

    public boolean isLogPosOccupied(long entityId, int cellNumber) {
        return megastore.getEntity(entityId).getLog().isOccupied(cellNumber);
    }
}
