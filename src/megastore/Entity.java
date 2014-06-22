package megastore;

import megastore.network.ListeningThread;
import megastore.network.message.paxos_optimisation.AreYouUpToDateMessage;
import megastore.network.message.paxos_optimisation.RequestValidLogCellsMessage;
import megastore.paxos.proposer.PaxosProposer;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;
import megastore.write_ahead_log.WriteOperation;
import systemlog.LogBuffer;
import systemlog.SystemLog;
import systemlog.SystemLogCell;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 12/05/2014.
 */
public class Entity {
    public  static final long rangeSize=10000;
    private long startingHashPoint;
    private List<String> nodesURL;
    private Log log;
    private Megastore megastore;


    public Entity(List<String> nodesURL, Megastore megastore, long startingHashPoint) {
        this.startingHashPoint = startingHashPoint;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        this.log=new Log(this);
    }

    public Entity(List<String> nodesURL, Megastore megastore, long startingHashPoint, Log log) {
        this.startingHashPoint = startingHashPoint;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        this.log=log;
    }


    public boolean put( String key, String value) {

        long hash=getHashValue(key);
        int currentPosition=log.getNextPosition();
        PaxosProposer proposer=new PaxosProposer(startingHashPoint, currentPosition, megastore, nodesURL);
        ListeningThread currentThread=megastore.getThread();
        currentThread.addProposer(proposer);

        boolean writeOperationResult = true;
        ValidLogCell cell = createLogCell(hash, value);
//      Accept Leader: Ask the leader to accept the value as proposal number zero.
//      The leader is the node that succeded the last write.

        String lastPostionsLeaderURL;

        if(currentPosition==0 || log.get(currentPosition-1) == null ||
                (! log.get(currentPosition-1).isValid() )  )
            lastPostionsLeaderURL=megastore.getNetworkManager().getNodesURL().get(0);
        else
            lastPostionsLeaderURL = log.get(currentPosition-1).getLeaderUrl();

//        boolean leaderProposalResult = proposer.proposeValueToLeader(lastPostionsLeaderURL, cell);
        boolean leaderProposalResult=false; //to disable optimisation

        if (leaderProposalResult) {
            proposer.proposeValueEnforced(cell, lastPostionsLeaderURL);
            SystemLog.add(new SystemLogCell(megastore.getCurrentUrl(), "One Round"));
        }
        else {
            writeOperationResult = proposer.proposeValueTwoPhases(cell);
            SystemLog.add(new SystemLogCell(megastore.getCurrentUrl(), "Two Rounds"));
        }

        currentThread.removeProposer(proposer);
        return writeOperationResult;
    }

    private ValidLogCell createLogCell(long key, String value) {
        List<WriteOperation> list = new LinkedList<WriteOperation>();
        list.add(new WriteOperation(key,value));
        return new ValidLogCell(megastore.getCurrentUrl(), list);
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
        log.append(logCell, cellNumber);
        // TODO also start a thread to write it on disk.
    }

    public Log getLog() {
        return log;
    }

    public long getEntityID() {
        return startingHashPoint;
    }

    public String get(String key) {
        long hash=getHashValue(key);

//        1.Query Local: Query the local replica's coordinator to determine if the entity group is up-to-date locally.
        if( megastore.getCoordinator().isUpToDate(startingHashPoint) ) {
            return getLocalLastValue(hash);
        }
        else {
            LogBuffer.println("Catch-up on node: " + megastore.getCurrentUrl());
            catchUp();
            megastore.getCoordinator().validate(startingHashPoint);
            return getLocalLastValue(hash);
        }
    }

    public void catchUp() {
        String nodeURL = findAnUpToDateNode();
        if(nodeURL !=null)
            updateMissingLogCellsFrom(nodeURL);
    }

    private String getLocalLastValue(long hash) {
        // read the highest accepted systemlog position and timestamp from the local replica.
        String value=log.getLastValueOf(hash);
        if(value == null) {
            // it means that there weren't modifications in the near past for that key and so
            // we have to read from the disk (bigtable).
            return null;
        }
        else
            return value;
    }

    private LinkedList<LogCell> newCells;
    private void updateMissingLogCellsFrom( String nodeURL) {
        newCells=null;

        int currentSize=log.getNextPosition();
        List<Integer> invalidPositions=log.getInvalidPositions();
        new RequestValidLogCellsMessage(startingHashPoint, null,
                megastore.getCurrentUrl(), nodeURL, invalidPositions,currentSize).send();

        try {
            do {
                if(newCells==null)
                    Thread.sleep(3);
            } while(newCells==null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i=0; i<invalidPositions.size(); i++) {
            log.append(newCells.get(i), invalidPositions.get(i));
        }
        for(int i=invalidPositions.size(), j=0; i<newCells.size(); i++, j++) {
            log.append(newCells.get(i), currentSize+j);
        }
    }

    private String upToDateNode;
    private String findAnUpToDateNode() {
        upToDateNode =null;

        for (String url : nodesURL) {
            if (!  megastore.getCurrentUrl().equals(url)  )
                new AreYouUpToDateMessage(startingHashPoint, null,
                        megastore.getCurrentUrl(), url).send();
        }

        long time=System.currentTimeMillis();
        try {
            do {
                if(upToDateNode==null)
                    Thread.sleep(3);
            } while(upToDateNode==null &&
                    System.currentTimeMillis()-time<200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return upToDateNode;
    }

    public void addUpToDateNode(String source) {
        if(upToDateNode ==null) // we only allow the first (fastest) node to register;
            upToDateNode =source;
    }

    public void setNewCells(LinkedList<LogCell> list) {
        newCells=list;
    }

    public void setMegastore(Megastore megastore) {
        this.megastore = megastore;
    }

    public Megastore getMegastore() {
        return megastore;
    }
}
