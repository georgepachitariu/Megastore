package megastore.write_ahead_log;

import megastore.Entity;
import megastore.LogBuffer;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private Entity parent;
    private LogCell[] logList;
    public int size;

    public Log(Entity parent) {
        logList=new LogCell[10000];
        size =0;
        this.parent=parent;
    }

    public void append(LogCell cell, int cellNumber) {
        synchronized (this) {
            if (logList[cellNumber] != null)
                LogBuffer.println("Node/Position " + parent.getMegastore().getCurrentUrl() + "/" + cellNumber +
                        "; Old: " + logList[cellNumber] +
                        "; New: " + cell);
            else
                LogBuffer.println("Node/Position: " + parent.getMegastore().getCurrentUrl() + "/" + cellNumber +
                        "; New: " + cell);

            logList[cellNumber] = cell;

            if (size == cellNumber)
                size++;
            if (size < cellNumber)
                size = cellNumber + 1;
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

    public boolean isOccupied(int cellNumber) {
        synchronized (this) {
            if (logList[cellNumber] != null && logList[cellNumber].isValid())
                return true;
            return false;
        }
    }

    public void setParent(Entity parent) {
        this.parent = parent;
    }
}
