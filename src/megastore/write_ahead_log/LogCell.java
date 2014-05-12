package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

public class LogCell {
    public List<WriteOperation> logList;

    public LogCell(List<WriteOperation> logList) {
        this.logList = logList;
    }

    public LogCell(String raw) {
        logList=new LinkedList<WriteOperation>();
        String[] parts = raw.split("q");
        for(int i=0; i < parts.length; i++)
            logList.add(new WriteOperation(parts[i]));
     }

    @Override
    public String toString() {
        String str="";
        for(int i=0; i < logList.size(); i++)
            str +=logList.get(i).toString() + "q";
        return str;
    }

    @Override
    public boolean equals(Object obj) {
        if(! (obj instanceof LogCell))
            return false;
        LogCell cell=(LogCell)obj;
        return toString().equals(cell.toString());
    }
}
