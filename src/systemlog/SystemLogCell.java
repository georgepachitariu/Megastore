package systemlog;

/**
 * Created by George on 17/06/2014.
 */
public class SystemLogCell extends LogCell {
    public String message;

    public SystemLogCell(String nodeUrl, String message) {
        super(nodeUrl);
        this.message=message;
    }

    @Override
    public String toString() {
        return nodeUrl+"  "+message;
    }

}
