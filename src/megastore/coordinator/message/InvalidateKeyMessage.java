package megastore.coordinator.message;

import megastore.Megastore;
import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;

/**
 * Created by George on 13/05/2014.
 */
public class InvalidateKeyMessage extends NetworkMessage {

    private final NetworkManager networkManager;
    private long entityID;

    public InvalidateKeyMessage(NetworkManager networkManager,
                                String destination, long entityID) {
        super(destination);
        this.entityID=entityID;
        this.networkManager=networkManager;
    }

    public static class CatchUpThread implements Runnable {
        private final long entityID;
        private final Megastore m;

        public CatchUpThread(Megastore m, long entityID) {
            this.m=m;
            this.entityID=entityID;
        }

        @Override
        public void run() {
            m.getEntity(entityID).catchUp();
            m.getCoordinator().validate(entityID);
        }
    }


    @Override
    public void act(String[] messageParts) {
        entityID=Long.parseLong( messageParts[1]);
        Megastore megastore = networkManager.getMegastore();
            megastore.invalidate(entityID);
        //Runnable r=new CatchUpThread(megastore,entityID);
        //new Thread(r).start();                                                                 //My optimisation
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
