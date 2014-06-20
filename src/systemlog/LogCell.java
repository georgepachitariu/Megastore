package systemlog;

/**
 * Created by George on 18/06/2014.
 */
public abstract class LogCell {
    public String nodeUrl;

    public LogCell(String nodeUrl) {
        this.nodeUrl=nodeUrl;
    }

    public String getNode() {
        return nodeUrl;
    }
}
