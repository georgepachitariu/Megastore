package megastore.paxos.message.phase2;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.write_ahead_log.ValidLogCell;

/**
 * Created by George on 13/05/2014.
 */
public class EnforcedAcceptRequest extends PaxosProposerMessage {
    private String sourceURL;
    private ValidLogCell value;

    public EnforcedAcceptRequest( long entityId, int cellNumber, NetworkManager networkManager,
                          String sourceURL, String destinationURL, ValidLogCell value) {
        super(networkManager,destinationURL,entityId,cellNumber);
        this.value=value;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        entityId=Long.parseLong( messageParts[0] );
        cellNumber=Integer.parseInt( messageParts[1] );

        ValidLogCell value = new ValidLogCell( messageParts[3] );
        String source= messageParts[4];

        networkManager.writeFinalValueOnLog(entityId, cellNumber, value); //we also set the final value
        new EnforcedAR_Accepted(entityId, cellNumber, null, networkManager.getCurrentUrl(), source).send();
    }

    @Override
    public String getID() {
        return "EnforcedAcceptRequest";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + value.toString() + "," + sourceURL;
    }
}