package megastore.paxos.message;

import megastore.network.message.NetworkMessage;

/**
 * Created by George on 02/05/2014.
 */
public class NullMessage extends NetworkMessage {
    public NullMessage(String destinationURL) {
        super(destinationURL);
    }

    @Override
    public void act(String[] messageParts) {
    }

    @Override
    public String getID() {
        return "NullMessage";
    }

    @Override
    protected String toMessage() {
        return getID();
    }
}