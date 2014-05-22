package megastore.write_ahead_log;

public abstract class LogCell {
    public LogCell(String leaderUrl) {
        this.leaderUrl = leaderUrl;
    }

    protected String leaderUrl;

    public String getLeaderUrl() {
        return leaderUrl;
    }

    public abstract String  getValue(long key);

    public abstract boolean isInvalid();
}
