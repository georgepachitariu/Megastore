package megastore.network.message;

import megastore.network.NetworkManager;

/**
 * Created by George on 11/05/2014.
 */
public class IntroductionMessage extends NetworkMessage {
    private final NetworkManager networkManager;
    private final String source;

    public IntroductionMessage(NetworkManager networkManager, String sourceURL, String destinationURL) {
        super(destinationURL);
        this.networkManager=networkManager;
        this.source=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
           String source=messageParts[1];
            networkManager.addNode(source);
           String nodes=networkManager.getAvailableNodesAsString();
           new AvailableNodesMessage(networkManager, source, nodes).send();
    }

    @Override
    public String getID() {
        return "IntroductionMessage";
    }

    @Override
    protected String toMessage() {
        return getID() + "," + source;
    }
}
