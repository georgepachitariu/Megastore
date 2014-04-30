package megastore.write_ahead_log;


public class WriteOperation {
    private long key;
    private Object newValue;

    public WriteOperation(long key, Object newValue) {
        this.key = key;
        this.newValue = newValue;
    }
}
