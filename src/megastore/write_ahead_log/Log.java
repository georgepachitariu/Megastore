package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private LogCell[] logList;
    public int occupied;

    public Log() {
        logList=new LogCell[10000];
        occupied=0;
    }

    public void append(LogCell cell, int cellNumber) {
        logList[cellNumber] = cell;
        if(occupied==cellNumber)
            occupied++;
        if(occupied<cellNumber)
            occupied=cellNumber+1;
    }

    public int getNextPosition() {
        return occupied;
    }

    public LogCell get(int i) {
        return logList[i];
    }

    public String getLastValueOf(long key) {
        for(int i=occupied-1; i>=0; i--) {
            String val = logList[i].getValue(key);
            if(val!=null)
                return val;
        }
        return null;
    }

    public List<Integer> getInvalidPositions() {
        LinkedList<Integer> invalidPositions=new LinkedList<Integer>();
        for(int i=0; i< occupied; i++) {
            if(logList[i].isInvalid())
                invalidPositions.add(i);
        }
        return invalidPositions;
    }
}
