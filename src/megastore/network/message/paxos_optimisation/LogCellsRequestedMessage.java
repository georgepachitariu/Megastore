package megastore.network.message.paxos_optimisation;

import megastore.Entity;
import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;
import megastore.write_ahead_log.LogCell;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 22/05/2014.
 */
public class LogCellsRequestedMessage extends NetworkMessage {
    private final NetworkManager networkManager;
    private final List<LogCell> list;
    private long entityID;

    public LogCellsRequestedMessage(long entityID, NetworkManager networkManager,
                                    String destination, LinkedList<LogCell> list) {
        super(destination);
        this.entityID=entityID;
        this.networkManager=networkManager;
        this.list=list;
    }

    @Override
    public void act(String[] messageParts) {
        entityID=Long.parseLong( messageParts[1] );

        Entity entity = networkManager.getMegastore().getEntity(entityID);

        LinkedList<LogCell> list=new LinkedList<LogCell>();
        for(int i=2; i<messageParts.length; i++) {
            LogCell cell= LogCell.createCell(messageParts[i]);
            list.add(cell);
        }
        entity.setNewCells(list);
    }

    @Override
    public String getID() {
        return "LogCellsRequestedMessage";
    }

    @Override
    protected String toMessage() {
        String blob=  getID() + "," + entityID;
        for(int i=0; i<list.size(); i++)
            blob+=","+list.get(i).toString();
        return blob;
    }
}