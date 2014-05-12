package megastore.network.message;

import megastore.network.NetworkManager;

/**
 * Created by George on 11/05/2014.
 */
public class AvailableNodesMessage extends NetworkMessage {

    private final String nodes;
    private final NetworkManager networkManager;

    public AvailableNodesMessage(NetworkManager networkManager,
                                 String destination, String nodes) {
        super(destination);
        this.nodes=nodes;
        this.networkManager=networkManager;
    }

    @Override
    public void act(String[] messageParts) {
        String nodes=messageParts[1];
        networkManager.putNodes(nodes);
    }

    @Override
    public String getID() {
        return "AvailableNodesMessage";
    }

    @Override
    protected String toMessage() {
        return getID() + "," + nodes;
    }
}
