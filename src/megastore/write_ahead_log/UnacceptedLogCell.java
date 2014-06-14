package megastore.write_ahead_log;

/**
 * Created by George on 13/05/2014.
 */
public class UnacceptedLogCell extends LogCell{
    public UnacceptedLogCell() {
    }

    @Override
    public String getLeaderUrl() {
        return null;
    }

    public String  getValue(long key) {
        return null;
    }

    public boolean isValid() {
        return false;
    }

    @Override
    public String toString() {
        return "UnacceptedLogCell";
    }
}
