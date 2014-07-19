package megastore.write_ahead_log;

/**
 * Created by George on 13/05/2014.
 */
public class InvalidLogCell extends LogCell{
    public InvalidLogCell() {
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
        if(! (obj instanceof InvalidLogCell))
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
        return true;
    }
}
