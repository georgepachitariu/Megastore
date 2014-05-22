package megastore;

import megastore.network.ListeningThread;
import megastore.network.message.paxos_optimisation.AreYouUpToDateMessage;
import megastore.network.message.paxos_optimisation.RequestValidLogCellsMessage;
import megastore.paxos.proposer.PaxosProposer;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;
import megastore.write_ahead_log.WriteOperation;

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
        this(nodesURL,megastore,startingHashPoint,new Log());
    }

    public Entity(List<String> nodesURL, Megastore megastore, long startingHashPoint, Log log) {
        this.startingHashPoint = startingHashPoint;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        this.log=log;
    }


    public boolean put( String key, String value) {

        long hash=getHashValue(key);
        PaxosProposer proposer=new PaxosProposer(startingHashPoint, log.getNextPosition(), megastore, nodesURL);
        ListeningThread currentThread=megastore.getThread();
        currentThread.addProposer(proposer);

        boolean writeOperationResult = true;
        ValidLogCell cell = createLogCell(hash, value);
//      Accept Leader: Ask the leader to accept the value as proposal number zero.
//      The leader is the node that succeded the last write.
        int lastPosition=log.getNextPosition()-1;
        if(lastPosition==-1) { // if there wasn't any value proposed before there isn't any leader
            writeOperationResult=proposer.proposeValueTwoPhases(cell);
            System.out.println("Two Rounds");
        }
        else {
            String lastPostionsLeaderURL = log.get(lastPosition).getLeaderUrl();
            boolean leaderProposalResult = proposer.proposeValueToLeader(lastPostionsLeaderURL, cell);

            if (leaderProposalResult) {
                proposer.proposeValueEnforced(cell, lastPostionsLeaderURL);
                System.out.println("One Round");
            }
            else {
                writeOperationResult = proposer.proposeValueTwoPhases(cell);
                System.out.println("Two Rounds");
            }
        }

        log.append(proposer.getFinalValue(), log.getNextPosition());
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
        // this came from another node in the network
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
            String nodeURL = findAnUpToDateNode();
            updateMissingLogCellsFrom(nodeURL);
            megastore.getCoordinator().validate(startingHashPoint);
            return getLocalLastValue(hash);
        }
    }

    private String getLocalLastValue(long hash) {
        // read the highest accepted log position and timestamp from the local replica.
        String value=log.getLastValueOf(hash);
        if(value == null) {
            // it means that there weren't modifications in the near past for that key and so
            // we have to read from the disk (bigtable).
            return null;
        }
        else
            return value;
    }

    private LinkedList<ValidLogCell> newCells;
    private void updateMissingLogCellsFrom( String nodeURL) {
        newCells=null;

        List<Integer> invalidPositions=log.getInvalidPositions();
        new RequestValidLogCellsMessage(startingHashPoint, null,
                megastore.getCurrentUrl(), nodeURL, invalidPositions,log.getNextPosition()).send();

        try {
            do {
                Thread.sleep(3);
            } while(newCells==null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i=0; i<invalidPositions.size(); i++) {
            log.append(newCells.get(i), invalidPositions.get(i));
        }
        for(int i=log.getNextPosition(); i<newCells.size(); i++) {
            log.append(newCells.get(i), log.getNextPosition());
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

        try {
            do {
                Thread.sleep(3);
            } while(upToDateNode==null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return upToDateNode;
    }

    public void addUpToDateNode(String source) {
        if(upToDateNode ==null) // we only allow the first (fastest) node to register;
            upToDateNode =source;
    }

    public void setNewCells(LinkedList<ValidLogCell> list) {
        newCells=list;
    }

    public void setMegastore(Megastore megastore) {
        this.megastore = megastore;
    }
}
