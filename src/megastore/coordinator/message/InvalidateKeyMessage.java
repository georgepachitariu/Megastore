package megastore.coordinator.message;

import megastore.Megastore;
import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;

/**
 * Created by George on 13/05/2014.
 */
public class InvalidateKeyMessage extends NetworkMessage {

    private NetworkManager networkManager;
    private long entityID;

    public InvalidateKeyMessage(NetworkManager networkManager,
                                 String destination, long entityID) {
        super(destination);
        this.entityID=entityID;
        this.networkManager=networkManager;
    }

    @Override
    public void act(String[] messageParts) {
        long entityID=Long.parseLong( messageParts[1]);
        Megastore megastore = networkManager.getMegastore();
        megastore.invalidate(entityID);

//        megastore.getEntity(entityID).catchUp();
//        megastore.getCoordinator().validate(entityID);
    }

    @Override
    public String getID() {
        return "InvalidateKeyMessage";
    }

    @Override
    protected String toMessage() {
        return getID() + "," + entityID;
    }
}
