package megastore.network.message.paxos_optimisation;

import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;

/**
 * Created by George on 22/05/2014.
 */
public class UpToDateConfirmedMessage extends NetworkMessage {

    private final NetworkManager networkManager;
    private String source;
    private long entityID;

    public UpToDateConfirmedMessage(long entityID,
                                 NetworkManager networkManager, String source, String destination) {
        super(destination);
        this.entityID=entityID;
        this.source=source;
        this.networkManager=networkManager;
    }

    @Override
    public void act(String[] messageParts) {
        entityID=Long.parseLong( messageParts[1] );
        source=messageParts[2];

        networkManager.getMegastore().getEntity(entityID).addUpToDateNode(source);
    }

    @Override
    public String getID() {
        return "UpToDateConfirmedMessage";
    }

    @Override
    protected String toMessage() {
        return getID() + "," + entityID+","+source;
    }
}