package megastore.write_ahead_log;

public class LogCell {
    public LogCell(String leaderUrl) {
        this.leaderUrl = leaderUrl;
    }

    protected String leaderUrl;

    public String getLeaderUrl() {
        return leaderUrl;
    }
}
