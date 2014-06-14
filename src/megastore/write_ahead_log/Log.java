package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private LogCell[] logList;
    public int size;

    public Log() {
        logList=new LogCell[10000];
        size =0;
    }

    public void append(LogCell cell, int cellNumber) {
        if(logList[cellNumber]!=null && logList[cellNumber].isValid() )
            System.out.println("We are overwriting on position: "+ cellNumber + " this:" + logList[cellNumber].toString() +
                                                            "with  " + cell.toString());

        logList[cellNumber] = cell;
        if(size ==cellNumber)
            size++;
        if(size <cellNumber)
            size =cellNumber+1;
    }

    public int getNextPosition() {
        return size;
    }

    public LogCell get(int i) {
        return logList[i];
    }

    public String getLastValueOf(long key) {
        for(int i= size -1; i>=0; i--) {
            if(logList[i]!=null) {
                String val = logList[i].getValue(key);
                if (val != null)
                    return val;
            }
        }
        return null;
    }

    public List<Integer> getInvalidPositions() {
        LinkedList<Integer> invalidPositions=new LinkedList<Integer>();
        for(int i=0; i< size; i++) {
            if(logList[i]==null || (! logList[i].isValid()))
                invalidPositions.add(i);
        }
        return invalidPositions;
    }

    @Override
    public String toString() {
        String blob="";
        for(int i= size; i>=0; i--) {
            if(logList[i]!=null) {
                blob+=logList[i].toString();
            }
        }
        return blob;
    }

    public boolean isOccupied(int cellNumber) {
        if(logList[cellNumber]!=null && logList[cellNumber].isValid())
            return true;
        return false;
    }
}
