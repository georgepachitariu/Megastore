package megastore.write_ahead_log;

public abstract class LogCell {
    public LogCell(){
    }

    public abstract String getLeaderUrl();

    public abstract String  getValue(long key);

    public abstract boolean isValid();

    public static LogCell createCell(String raw) {
        if(raw.equals("InvalidLogCell"))
            return new InvalidLogCell();
        else
            return new ValidLogCell(raw);
    }

    public abstract boolean isPositionOpenedForWeakerProposals();

    public abstract boolean isLocalOperationAccepted();
}
