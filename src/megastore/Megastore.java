package megastore;

import systemlog.LogBuffer;
import megastore.coordinator.Coordinator;
import megastore.network.ListeningThread;
import megastore.network.NetworkManager;
import megastore.network.message.NewEntityMessage;
import megastore.paxos.acceptor.PaxosAcceptor;
import megastore.write_ahead_log.LogCell;

import java.util.LinkedList;
import java.util.List;

public class Megastore {
    private NetworkManager networkManager;
    private List<Entity> existingEntities;
    private Coordinator coordinator;

    public Megastore(String port) {
        networkManager=new NetworkManager(this, port);
        existingEntities=new LinkedList<Entity>();
        coordinator=new Coordinator();
    }

    public Megastore(String port, String existingNodeAdress) {
        networkManager=new NetworkManager(this, port);
        networkManager.updateNodesFrom(existingNodeAdress);
        existingEntities=new LinkedList<Entity>();
        coordinator=new Coordinator();
    }

    public Megastore(String port, List<Entity> existingEntities) {
        this(port);
        this.existingEntities=existingEntities;
    }

    public Megastore(String port, String existingNodeAdress, List<Entity> existingEntities) {
        this(port,existingNodeAdress);
        this.existingEntities=existingEntities;
    }

    public Entity createEntity() {
        long entityID=existingEntities.size()*Entity.rangeSize;
        Entity e = new Entity(networkManager.getNodesURL(), this, entityID);
        existingEntities.add(e);

        List<String> list = networkManager.getNodesURL();
        for(String s: list)
               if(!getCurrentUrl().equals(s))
                    new NewEntityMessage(networkManager, s, e.getEntityID(),
                            networkManager.getNodesURL()).send();

        coordinator.addEntity(e.getEntityID());
        return e;
    }

    public ListeningThread getListeningThread() {
        return networkManager.getListeningThread();
    }

    public String getCurrentUrl() {
       return networkManager.getCurrentUrl();
    }

    public PaxosAcceptor getAcceptor(long entityId, int cellNumber) {
        List<PaxosAcceptor> list = networkManager.getListeningThread().getAcceptorSession();
        for(PaxosAcceptor p : list) {
            if(p.isTheRightSession(entityId,cellNumber))
                return p;
        }
        return null;
    }

    public void append(long entityId, int cellNumber, LogCell value) {
        boolean entityFound=false;
        for(Entity e : existingEntities)
            if(e.getEntityID()==entityId) {
                e.appendToLog(value, cellNumber);
                entityFound=true;
            }
        if(! entityFound)
            LogBuffer.println("Entity not found");
    }

    public Entity getEntity(long entityId) {
        for(Entity e : existingEntities)
            if(e.getEntityID()==entityId)
                return e;
        return null;
    }

    public List<Entity> getExistingEntities() {
        return existingEntities;
    }

    public void close() {
        networkManager.close();
    }

    public void addEntity(long entityID, List<String> urls) {
        Entity e=new Entity(urls, this, entityID);
        existingEntities.add(e);
        coordinator.addEntity(e.getEntityID());
    }

    public void invalidate(long entityID) {
        coordinator.invalidate(entityID);
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public NetworkManager getNetworkManager() {
        return networkManager;
    }

}
