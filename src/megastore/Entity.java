package megastore;

import megastore.network.ListeningThread;
import megastore.paxos.proposer.PaxosProposer;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.WriteOperation;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 12/05/2014.
 */
public class Entity {
    private static final long rangeSize=10000;
    private long startingHashPoint;
    private List<String> nodesURL;
    private Log log;

    private Megastore megastore;

    public Entity(List<String> nodesURL, Megastore megastore, long startingHashPoint) {
        this.startingHashPoint = startingHashPoint;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        log=new Log();
    }

    public boolean put( String key, String value) {
        long hash=getHashValue(key);

        // 1. make paxos to get consensus on all required nodes
        ListeningThread currentThread=megastore.getThread();
        PaxosProposer p=new PaxosProposer(startingHashPoint, 0, megastore, nodesURL);
        currentThread.addProposer(p);
        LogCell cell = getLogCell(hash,value);
        p.proposeValue(cell);
        if(! p.getFinalValue().equals(cell) ) {
            return false;    // it failed
        }

        // 2. on success: write it on log (which should be written down on disk)
        log.append(p.getFinalValue(), log.getNextPosition());

        // 3. make the write on bigtable

        return true;
    }

    private LogCell getLogCell(long key, String value) {
        List<WriteOperation> list = new LinkedList<WriteOperation>();
        list.add(new WriteOperation(key,value));
        return new LogCell(list);
    }

    public long getHashValue(String key) {
        char[] chars=key.toCharArray();
        long hash=7;
        for (int i=0; i < chars.length; i++) {
            hash = hash*31+chars[i];
        }

        hash=hash%rangeSize;
        hash+=startingHashPoint;
        return hash;
    }


    public void appendToLog(LogCell logCell, int cellNumber) {
        // this came from another node in the network
        log.append(logCell, cellNumber);
        // also start a thread to write it on disk.
    }

    public Log getLog() {
        return log;
    }

    public long getEntityID() {
        return startingHashPoint;
    }

    public LogCell get(String white) {
        return null;
        // TODO
    }
}
