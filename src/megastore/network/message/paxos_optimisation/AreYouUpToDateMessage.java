package megastore.network.message.paxos_optimisation;

import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;

/**
 * Created by George on 22/05/2014.
 */
public class AreYouUpToDateMessage extends NetworkMessage {

    private final NetworkManager networkManager;
    private String source;
    private long entityID;

    public AreYouUpToDateMessage(long entityID,
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

       if( networkManager.getMegastore().getCoordinator().isUpToDate(entityID) )
        //We changed what up-to-date means: Now it means that if the node has all records valid
            new UpToDateConfirmedMessage(entityID,null, networkManager.getCurrentUrl(),source).send();
    }

    @Override
    public String getID() {
        return "AreYouUpToDateMessage";
    }

    @Override
    protected String toMessage() {
        return getID() + "," + entityID+","+source;
    }
}

