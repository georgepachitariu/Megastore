package megastore.write_ahead_log;

import java.util.List;

public class LogCell {
    private List<WriteOperation> logList;
    private boolean isCommited;

    public LogCell(List<WriteOperation> logList) {
        this.logList = logList;
        isCommited=false;
    }

    public void recordCommit() {
        isCommited=true;
    }
}
