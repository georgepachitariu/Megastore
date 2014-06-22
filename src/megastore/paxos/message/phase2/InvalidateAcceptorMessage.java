package megastore.paxos.message.phase2;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.InvalidLogCell;

/**
 * Created by George on 14/06/2014.
 */
public class InvalidateAcceptorMessage extends PaxosProposerMessage {
    private String sourceURL;

    public InvalidateAcceptorMessage(long entityId, int cellNumber, NetworkManager networkManager,
                                     String sourceURL, String destinationURL) {
        super(networkManager,destinationURL,entityId,cellNumber);
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        entityId=Long.parseLong( messageParts[0] );
        cellNumber=Integer.parseInt( messageParts[1] );

        String source= messageParts[3];

        Log log=networkManager.getMegastore().getEntity(entityId).getLog();
        if(log.get(cellNumber) !=null && source.equals(log.get(cellNumber).getLeaderUrl()))
            log.append(new InvalidLogCell(),cellNumber);
    }

    @Override
    public String getID() {
        return "InvalidateAcceptorMessage";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID() + "," + sourceURL;
    }
}