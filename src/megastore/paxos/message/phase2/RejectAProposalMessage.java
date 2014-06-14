package megastore.paxos.message.phase2;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.UnacceptedLogCell;

/**
 * Created by George on 14/06/2014.
 */
public class RejectAProposalMessage extends PaxosProposerMessage {
    private String sourceURL;

    public RejectAProposalMessage(long entityId, int cellNumber, NetworkManager networkManager,
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
            log.append(new UnacceptedLogCell(),cellNumber);
    }

    @Override
    public String getID() {
        return "AcceptRequest";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID() + "," + sourceURL;
    }
}