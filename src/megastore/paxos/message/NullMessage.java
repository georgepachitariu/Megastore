package megastore.paxos.message;

/**
 * Created by George on 02/05/2014.
 */
public class NullMessage extends PaxosAcceptorMessage {
    public NullMessage(String destinationURL) {
        super(null, destinationURL,0,0);
    }

    @Override
    public void act(String[] messageParts) {
    }

    @Override
    public String getID() {
        return "";
    }

    @Override
    protected String toMessage() {
        return "";
    }
}
