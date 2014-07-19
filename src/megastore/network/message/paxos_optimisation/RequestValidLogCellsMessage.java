package megastore.network.message.paxos_optimisation;

import megastore.Entity;
import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;
import megastore.write_ahead_log.InvalidLogCell;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.LogCell;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 22/05/2014.
 */
public class RequestValidLogCellsMessage extends NetworkMessage {

    private NetworkManager networkManager;
    private String source;
    private long entityID;
    private List<Integer> positions;
    private int logSize;

    public RequestValidLogCellsMessage(long entityID,NetworkManager networkManager,
                                       String source, String destination, List<Integer> positions, int logSize) {
        super(destination);
        this.entityID=entityID;
        this.source=source;
        this.networkManager=networkManager;
        this.positions=positions;
        this.logSize=logSize;
    }

    @Override
    public void act(String[] messageParts) {
        entityID=Long.parseLong( messageParts[1] );
        source=messageParts[2];
        logSize=Integer.parseInt( messageParts[3] );


        Entity en;
        do {
            en=networkManager.getMegastore().getEntity(entityID);
            if(en==null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (en==null);
        Log log=en.getLog();
        LinkedList<LogCell> list=new LinkedList<LogCell>();
        for(int i=4; i<messageParts.length; i++) {
            int pos=Integer.parseInt( messageParts[i] );
            LogCell cell = log.get(pos);
            if(cell == null)
                cell=new InvalidLogCell();
            list.add(cell);
        }
        for(int pos=logSize; pos<log.getNextPosition(); pos++) {
            LogCell cell = log.get(pos);
            if(cell == null)
                cell=new InvalidLogCell();
            list.add(cell);
        }

        new LogCellsRequestedMessage(entityID, null,source, list).send();
    }

    @Override
    public String getID() {
        return "RequestValidLogCellsMessage";
    }

    @Override
    protected String toMessage() {
        String blob=getID() + "," + entityID+","+source+","+logSize;
        for(int i=0; i<positions.size(); i++)
            blob+="," + positions.get(i);
        return blob;
    }
}