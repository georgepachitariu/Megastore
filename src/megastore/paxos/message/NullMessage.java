package megastore.paxos.message;

import megastore.paxos.Paxos;

/**
 * Created by George on 02/05/2014.
 */
public class NullMessage extends Message {
    public NullMessage(Paxos paxos, String destinationURL) {
        super(paxos, destinationURL);
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
