package megastore.write_ahead_log;

/**
 * Created by George on 13/05/2014.
 */
public class UnacceptedLogCell extends LogCell{
    public UnacceptedLogCell(String leaderUrl) {
        super(leaderUrl);
    }

    public String  getValue(long key) {
        return null;
    }

    public boolean isInvalid() {
        return true;
    }
}
