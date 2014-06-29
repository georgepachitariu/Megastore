package systemlog;

/**
 * Created by George on 17/06/2014.
 */
public class OperationLogCell extends LogCell {
    public final String value;
    public final boolean succeeded;
    public final long timestamp;
    public final long readDuration;
    public final long writeDuration;
    public final long timeToWaitForCompletion;

    public OperationLogCell(String nodeUrl, String value,  long readDuration,
                            long writeDuration, boolean succeeded, long timestamp, long timeToWaitForCompletion) {
        super(nodeUrl);
        this.value=value;
        this.readDuration=readDuration;
        this.writeDuration=writeDuration;
        this.succeeded=succeeded;
        this.timestamp = timestamp;
        this.timeToWaitForCompletion=timeToWaitForCompletion;
    }

    @Override
    public String toString() {
        return nodeUrl+"  "+"succeeded:  " +succeeded+ "     " +timestamp;
    }

}
