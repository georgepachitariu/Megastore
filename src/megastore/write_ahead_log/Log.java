package megastore.write_ahead_log;

import megastore.Entity;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private Entity parent;
    private LogCell[] logList;
    public int size;

    private int failedRequests;
    private long firstFailedTimestamp;

    public Log(Entity parent) {
        logList=new LogCell[30000];
        size =0;
        this.parent=parent;
        failedRequests=0;
        firstFailedTimestamp=System.currentTimeMillis();
    }

    public void append(LogCell cell, int cellNumber) {
        synchronized (this) {
//            if (logList[cellNumber] != null  )
//                LogBuffer.println("Node/Position " + parent.getMegastore().getCurrentUrl() + "/" + cellNumber +
//                        "; Old: " + logList[cellNumber] +
//                        "; New: " + cell);
//            else
//                LogBuffer.println("Node/Position: " + parent.getMegastore().getCurrentUrl() + "/" + cellNumber +
//                        "; New: " + cell);

            logList[cellNumber] = cell;

            if(cell!=null && cell.isValid()) {
                if (size == cellNumber)
                    size++;
                if (size < cellNumber)
                    size = cellNumber + 1;
            }
        }
    }

    public int getNextPosition() {
        synchronized (this) {
            return size;
        }
    }

    public LogCell get(int i) {
        synchronized (this) {
            return logList[i];
        }
    }

    public String getLastValueOf(long key) {
        synchronized (this) {
            for (int i = size - 1; i >= 0; i--) {
                if (logList[i] != null) {
                    String val = logList[i].getValue(key);
                    if (val != null)
                        return val;
                }
            }
            return null;
        }
    }

    public List<Integer> getInvalidPositions() {
        synchronized (this) {
            LinkedList<Integer> invalidPositions = new LinkedList<Integer>();
            for (int i = 0; i < size; i++) {
                if (logList[i] == null || (!logList[i].isValid()))
                    invalidPositions.add(i);
            }
            return invalidPositions;
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            String blob = "";
            for (int i = size; i >= 0; i--) {
                if (logList[i] != null) {
                    blob += logList[i].toString();
                }
            }
            return blob;
        }
    }

    public synchronized boolean isOccupied(int cellNumber) {
        if (logList[cellNumber] != null && logList[cellNumber].isValid()) {
            if(System.currentTimeMillis()-firstFailedTimestamp>200) {
                failedRequests = 0;
                firstFailedTimestamp=System.currentTimeMillis();
            }
            failedRequests++;

            if(failedRequests >6) {
                makeNextLogPositionToFavorOtherNodes();   //activation point of the "wait" optimisation
                failedRequests=0;
                firstFailedTimestamp=System.currentTimeMillis();
            }
            return true;
        }
        else {
            return false;
        }
    }

    private void makeNextLogPositionToFavorOtherNodes() {
        for(int i=size-1; ;i++)
            if(logList[i]==null) {
                logList[i] = new LogCellInFavorOfNotLocal();
                return;
            }
    }

    public void setParent(Entity parent) {
        this.parent = parent;
    }

    public boolean isPositionOpenedForWeakerProposals(int cellNumber) {
        if (logList[cellNumber] != null && logList[cellNumber].isPositionOpenedForWeakerProposals())
            return true;
        return false;
    }
}
