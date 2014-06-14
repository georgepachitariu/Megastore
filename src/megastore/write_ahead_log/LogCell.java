package megastore.write_ahead_log;

public abstract class LogCell {
    public LogCell(){
    }

    public abstract String getLeaderUrl();

    public abstract String  getValue(long key);

    public abstract boolean isValid();
}
