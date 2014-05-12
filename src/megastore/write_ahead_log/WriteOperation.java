package megastore.write_ahead_log;


public class WriteOperation {
    public long key;
    public String newValue;

    public WriteOperation(long key, String newValue) {
        this.key = key;
        this.newValue = newValue;
    }

    public WriteOperation(String raw) {
        String[] parts = raw.split("w");
        key=Long.parseLong(parts[0]);
        newValue=parts[1];
    }

    @Override
    public String toString() {
        return key+ "w" +newValue.toString();
    }
}
