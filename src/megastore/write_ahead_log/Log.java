package megastore.write_ahead_log;

import java.util.ArrayList;

public class Log {
    private ArrayList<LogCell> logList;

    public Log() {
        logList=new ArrayList<LogCell>(1000);
    }

    public void append(LogCell cell, int cellNumber) {
        logList.add(cell);
    }

    public int getNextPosition() {
        return logList.size();
    }

    public LogCell get(int i) {
        return logList.get(i);
    }
}
