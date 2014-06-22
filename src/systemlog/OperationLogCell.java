package systemlog;

/**
 * Created by George on 17/06/2014.
 */
public class OperationLogCell extends LogCell {
    public final boolean succeeded;
    public final long timestamp;
    public final long readDuration;
    public final long writeDuration;

    public OperationLogCell(String nodeUrl, long readDuration, long writeDuration, boolean succeeded, long timestamp) {
        super(nodeUrl);
        this.readDuration=readDuration;
        this.writeDuration=writeDuration;
        this.succeeded=succeeded;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return nodeUrl+"  "+"succeeded:  " +succeeded+ "     " +timestamp;
    }

}
