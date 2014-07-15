package megastore.write_ahead_log;

/**
 * Created by George on 05/07/2014.
 */
public class LogCellOpenedForWeak extends LogCell{
    public LogCellOpenedForWeak() {
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
        return "LogCellOpenedForWeak";
    }

    @Override
    public boolean equals(Object obj) {
        if(! (obj instanceof LogCellOpenedForWeak))
            return false;
        else
            return true;
    }

    @Override
    public boolean isPositionOpenedForWeakerProposals() {
        return true;
    }
}
