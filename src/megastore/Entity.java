package megastore;

import megastore.network.ListeningThread;
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
        PaxosProposer proposer=new PaxosProposer(startingHashPoint, log.getNextPosition(), megastore, nodesURL);
        ListeningThread currentThread=megastore.getThread();
        currentThread.addProposer(proposer);

//        1. Accept Leader: Ask the leader to accept the value as proposal number zero.
//            The leader is the node that succeded the last write.
        String lastPostionsLeaderURL=log.get(log.getNextPosition()-1).getLeaderUrl();
        ValidLogCell cell = createLogCell(hash, value);
        boolean leaderProposalResult=proposer.proposeValueToLeader(lastPostionsLeaderURL, cell);
        boolean writeOperationResult=true;

        if(leaderProposalResult)
            proposer.proposeValueEnforced(cell);
        else
            writeOperationResult=proposer.proposeValueTwoPhases(cell);

        // TODO get last value and put it in log

//        5. Apply: Apply the value's mutations at as many replicas as possible. If the chosen value diers from that
//        originally proposed, return a conflict error.
        // TODO
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
        // also start a thread to write it on disk.
    }

    public Log getLog() {
        return log;
    }

    public long getEntityID() {
        return startingHashPoint;
    }

    public LogCell get(String white) {
//        In preparation for a current read (as well as before a
//        write), at least one replica must be brought up to date: all
//        mutations previously committed to the log must be copied
//        to and applied on that replica. (this replica is selected)

//        1.Query Local: Query the local replica's coordinator to determine
//        if the entity group is up-to-date locally.
        megastore.getCoordinator().isUpToDate(startingHashPoint);

//        2. Find Position: Determine the highest possibly-committed log position,
//        and select a replica that has applied through that log position.
        //TODO

//        (a) (Local read) If step 1 indicates that the local replica is up-to-date,
//        read the highest accepted log position and timestamp from the local replica.


//        (b) (Majority read) If the local replica is not up-to-date (or if step 1 or step 2a times out),
//        read from a majority of replicas to nd the maximum log position that any replica
//        has seen, and pick a replica to read from. We select the most responsive
//        or up-to-date replica, not always the local replica.

//        3. Catchup: As soon as a replica is selected, catch it up to the maximum known log position as follows:

//        (a) For each log position in which the selected replica does not know the consensus value, read the
//        value from another replica. For any log positions without a known-committed value available, in-
//        voke Paxos to propose a no-op write. Paxos will drive a majority of replicas to converge on a single
//        value|either the no-op or a previously proposed write.

//        (b) Sequentially apply the consensus value of all unapplied log positions to advance the replica's state
//        to the distributed consensus state.

//        4. Validate: If the local replica was selected and was not previously up-to-date, send the coordinator a validate
//        message asserting that the (entity group; replica) pair reflects all committed writes. Do not wait for a reply|
//        if the request fails, the next read will retry.

//        5. Query Data: Read the selected replica using the timestamp of the selected log position. If the selected
//        replica becomes unavailable, pick an alternate replica, perform catchup, and read from it instead. The results
//        of a single large query may be assembled transparently from multiple replicas.
        return null;
    }
}
