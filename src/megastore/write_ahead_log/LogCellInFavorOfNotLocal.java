package megastore.write_ahead_log;

/**
 * Created by George on 15/07/2014.
 */
public class LogCellInFavorOfNotLocal extends LogCell {
    private long creatingTime;

    public LogCellInFavorOfNotLocal() {
        creatingTime=System.currentTimeMillis();
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
        return "InvalidLogCell";
    }

    @Override
    public boolean equals(Object obj) {
        if(! (obj instanceof LogCellInFavorOfNotLocal))
            return false;
        else
            return true;
    }

    @Override
    public boolean isPositionOpenedForWeakerProposals() {
        return false;
    }

    @Override
    public boolean isLocalOperationAccepted() {
        if(System.currentTimeMillis()-creatingTime>1000)
            return true;
        else
            return false;
    }
}
