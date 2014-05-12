package megastore.network.message;

import megastore.network.NetworkManager;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 12/05/2014.
 */
public class NewEntityMessage extends NetworkMessage {
    private List<String> urls;
    private  NetworkManager networkManager;
    private long entityID;

    public NewEntityMessage(NetworkManager networkManager,  String destinationURL, long entityID, List<String> urls) {
        super(destinationURL);
        this.networkManager=networkManager;
        this.entityID=entityID;
        this.urls=urls;
    }

    @Override
    public void act(String[] messageParts) {
        long entityID=Long.parseLong(messageParts[1]);
        urls=new LinkedList<String>();
        for(int i=2; i<messageParts.length; i++)
            urls.add(messageParts[i]);

        networkManager.getMegastore().addEntity(entityID,urls);
    }

    @Override
    public String getID() {
        return "NewEntityMessage";
    }

    @Override
    protected String toMessage() {
        String str=  getID() + "," + entityID+ ",";
        for(String u:  urls) {
            str+=u+",";
        }
        return str;
    }
}
