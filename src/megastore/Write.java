package megastore;

/**
 * Created by George on 19/07/2014.
 */
public class Write {
    public String key;
    public String newValue;
    public long creationTimeStamp;

    public Write(String key, String newValue) {
        this.key = key;
        this.newValue = newValue;
        this.creationTimeStamp=System.currentTimeMillis();
    }

}
