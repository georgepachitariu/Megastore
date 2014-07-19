package megastore;

import megastore.coordinator.message.InvalidateKeyMessage;
import megastore.network.ListeningThread;
import megastore.network.message.paxos_optimisation.AreYouUpToDateMessage;
import megastore.network.message.paxos_optimisation.RequestValidLogCellsMessage;
import megastore.paxos.proposer.PaxosProposer;
import megastore.write_ahead_log.*;
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
    private long entityId;
    private List<String> nodesURL;
    private Log log;
    private Megastore megastore;
    private boolean readyForNextWriteOperation;
    private boolean isNextWeak;


    public Entity(List<String> nodesURL, Megastore megastore, long entityId) {
        this.entityId = entityId;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        readyForNextWriteOperation=true;
        this.log=new Log(this);
        isNextWeak=false;
    }

    public Entity(List<String> nodesURL, Megastore megastore, long entityId, Log log) {
        this.entityId = entityId;
        this.megastore=megastore;
        this.nodesURL=nodesURL;
        readyForNextWriteOperation=true;
        this.log=log;
        isNextWeak=false;
    }

    public void blockNextOperation() {
        readyForNextWriteOperation=false;
    }

    public void put(String key, String value, DBWriteOp callback, boolean isWeak) {
        boolean lockOnceReleased=false;
        long hash=getHashValue(key);
        ValidLogCell cell = createLogCell(hash, value);

        int currentPosition=log.getNextPosition();
        PaxosProposer proposer=new PaxosProposer(entityId, currentPosition,
                megastore, nodesURL, cell, callback);
        ListeningThread currentThread=megastore.getListeningThread();
        currentThread.addProposer(proposer);

        boolean writeOperationResult=false;
//      Accept Leader: Ask the leader to accept the value as proposal number zero.
//      The leader is the node that succeded the last write.

        String lastPostionsLeaderURL;

        int lastLeader=0;
        if(currentPosition==0)
            lastPostionsLeaderURL=megastore.getNetworkManager().getNodesURL().get(0);
        else {
            lastLeader = currentPosition - 1;
            while (log.get(lastLeader)==null || (!log.get(lastLeader).isValid()))
                lastLeader--;
            lastPostionsLeaderURL = log.get(lastLeader).getLeaderUrl();
        }

        boolean leaderProposalResult = proposer.proposeValueToLeader(lastPostionsLeaderURL); //also comment this
        //    boolean leaderProposalResult=false; //to disable optimisation

        if (leaderProposalResult) {
            if(lastLeader>=1 &&                                                                                                          //<-my optimisation
                    lastPostionsLeaderURL.equals(megastore.getCurrentUrl())) {          //
                isNextWeak=true;                                                                                                           //
                readyForNextWriteOperation=true;                                                                              //
                lockOnceReleased=true;                                                                                              //
            }                                                                                                                                           //

            if(isWeak) {
                writeOperationResult = proposer.proposeValueWeak(lastPostionsLeaderURL);
            }
            else {
                writeOperationResult = proposer.proposeValueEnforced(lastPostionsLeaderURL);
            }
            SystemLog.add(new SystemLogCell(megastore.getCurrentUrl(), "One Round"));
        }
        else
        if(! isWeak) {
            writeOperationResult = proposer.proposeValueTwoPhases();
            SystemLog.add(new SystemLogCell(megastore.getCurrentUrl(), "Two Rounds"));
        }

        currentThread.removeProposer(proposer);
        isNextWeak=false;
        if(! lockOnceReleased) {
            readyForNextWriteOperation = true;
        }

        callback.setAnswer( writeOperationResult );
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
        hash+= entityId;
        return hash;
    }

    public void appendToLog(LogCell logCell, int cellNumber) {
        log.append(logCell, cellNumber);
    }

    public Log getLog() {
        return log;
    }

    public long getEntityID() {
        return entityId;
    }

    public String get(String key) {
        long hash=getHashValue(key);

//        1.Query Local: Query the local replica's coordinator to determine if the entity group is up-to-date locally.
        if (megastore.getCoordinator().isUpToDate(entityId)) {
            return getLocalLastValue(hash);
        } else {
            LogBuffer.println("Catch-up on node: " + megastore.getCurrentUrl());
            reValidate();

            return getLocalLastValue(hash);
        }
    }

    public void reValidate() {
        //       catchUp();
        //       megastore.getCoordinator().validate(entityId);
        Thread thr = new Thread(new InvalidateKeyMessage.CatchUpThread(megastore, entityId), "reValidate_Thr");
        thr.start();
        while ((!megastore.getCoordinator().isUpToDate(entityId)) &&  thr.isAlive()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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

    private final Object lock=new Object();
    private LinkedList<LogCell> newCells;
    private void  updateMissingLogCellsFrom( String nodeURL) {
        synchronized (lock) {
            newCells = null;
            int currentSize = log.getNextPosition();
            List<Integer> invalidPositions = log.getInvalidPositions();
            new RequestValidLogCellsMessage(entityId, null,
                    megastore.getCurrentUrl(), nodeURL, invalidPositions, currentSize).send();

            try {
                do {
                    if (newCells == null)
                        Thread.sleep(3);
                } while (newCells == null);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < invalidPositions.size() && i<newCells.size(); i++) {
                log.append(newCells.get(i), invalidPositions.get(i));
            }
            for (int i = invalidPositions.size(), j = 0; i < newCells.size(); i++, j++) {
                log.append(newCells.get(i), currentSize + j);
            }
        }
    }


    private String upToDateNode;
    private String findAnUpToDateNode() {
        upToDateNode =null;

        for (String url : nodesURL) {
            if (!  megastore.getCurrentUrl().equals(url)  )
                new AreYouUpToDateMessage(entityId, null,
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

    public synchronized void  setNewCells(LinkedList<LogCell> list) {
        newCells=list;
    }

    public void setMegastore(Megastore megastore) {
        this.megastore = megastore;
    }

    public Megastore getMegastore() {
        return megastore;
    }

    public boolean isReadyForNextWriteOperation() {
        return readyForNextWriteOperation;
    }

    public void proposeValueToLeaderAgainIfThereWasOne(long entityId, int cellNumber) {
        PaxosProposer oldProposer = megastore.getListeningThread().getProposer(entityId, cellNumber);

        if (oldProposer != null  && oldProposer.aquireSendingLock()) {
            ValidLogCell cell = oldProposer.getOriginalValue();

            int currentPosition = log.getNextPosition();
            PaxosProposer proposer = new PaxosProposer(this.entityId,
                    currentPosition, megastore, nodesURL, cell, oldProposer.getCallback());
            megastore.getListeningThread().addProposer(proposer);

            String lastPostionsLeaderURL=log.get(currentPosition - 1).getLeaderUrl();
            boolean leaderProposalResult = proposer.proposeValueToLeader(lastPostionsLeaderURL);

            if (leaderProposalResult) {
                boolean result = proposer.proposeValueEnforced(lastPostionsLeaderURL);
                if (result) {
                    SystemLog.add(new SystemLogCell(megastore.getCurrentUrl(), "One Round"));
                    oldProposer.getCallback().setAnswer(true);
                    oldProposer.operationHasBeenCompletedByAnotherThread();
                }
            }

            megastore.getListeningThread().removeProposer(proposer);
            oldProposer.releaseFasterSendingLock();
        }
    }

    public boolean isWritingLockWeak() {
        return isNextWeak;
    }

    public void makeCellOpenedForWeakProposals(int i) {
        log.append(new LogCellOpenedForWeak(), i);
    }

    public boolean isLocalOperationAccepted(int cellNumber) {
        LogCell cell = log.get(cellNumber);
        if(cell==null)
            return true;
        else
            return cell.isLocalOperationAccepted();
    }
}
