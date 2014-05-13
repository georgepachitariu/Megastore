package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 13/05/2014.
 */
public class ValidLogCell extends LogCell {
    public List<WriteOperation> logList;

    public ValidLogCell(String leaderUrl, List<WriteOperation> logList) {
        this.logList = logList;
        super.leaderUrl=leaderUrl;
    }

    public ValidLogCell(String raw) {
        logList=new LinkedList<WriteOperation>();
        String[] parts = raw.split("q");
        super.leaderUrl=parts[0];
        for(int i=1; i < parts.length; i++)
            logList.add(new WriteOperation(parts[i]));
    }

    @Override
    public String toString() {
        String str="leaderUrl"+"q";
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
