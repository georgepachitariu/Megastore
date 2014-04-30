package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

public class Log {
    private List<LogCell> logList;

    Log() {
        logList=new LinkedList<LogCell>();
    }

    public void append(LogCell cell) {
        logList.add(cell);
    }
}
