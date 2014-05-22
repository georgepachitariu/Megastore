package megastore.write_ahead_log;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 13/05/2014.
 */
public class ValidLogCell extends LogCell {
    public List<WriteOperation> opList;

    public ValidLogCell(String leaderUrl, List<WriteOperation> opList) {
        super(leaderUrl);
        this.opList = opList;
    }

    public ValidLogCell(String raw) {
        super("");
        opList =new LinkedList<WriteOperation>();
        String[] parts = raw.split("`");
        super.leaderUrl=parts[0];
        for(int i=1; i < parts.length; i++)
            opList.add(new WriteOperation(parts[i]));
    }

    @Override
    public String toString() {
        String str=leaderUrl+"`";
        for(int i=0; i < opList.size(); i++)
            str += opList.get(i).toString() + "`";
        return str;
    }

    @Override
    public boolean equals(Object obj) {
        if(! (obj instanceof ValidLogCell))
            return false;
        ValidLogCell cell=(ValidLogCell)obj;
        return toString().equals(cell.toString());
    }

    public String  getValue(long key) {
        for(WriteOperation wr: opList)
            if(wr.key==key)
                return wr.newValue;
        return null;
    }

    public boolean isInvalid() {
        return false;
    }
}
