package systemlog;

/**
 * Created by George on 17/06/2014.
 */
public class OperationLogCell extends LogCell {
    public final long duration;
    public final boolean succeeded;
    public final long timestamp;

    public OperationLogCell(String nodeUrl, long duration, boolean succeeded, long timestamp) {
        super(nodeUrl);
        this.duration=duration;
        this.succeeded=succeeded;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return nodeUrl+"  "+"succeeded:  " +succeeded+ "     " +timestamp;
    }

}
