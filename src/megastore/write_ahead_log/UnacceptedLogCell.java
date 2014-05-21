package megastore.write_ahead_log;

/**
 * Created by George on 13/05/2014.
 */
public class UnacceptedLogCell extends LogCell{
    public UnacceptedLogCell(String leaderUrl) {
        super(leaderUrl);
    }
}
