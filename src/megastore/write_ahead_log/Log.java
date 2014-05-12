package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private List<LogCell> logList;

    public Log() {
        logList=new LinkedList<LogCell>();
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
