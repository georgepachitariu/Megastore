package megastore.paxos.message.weakerRequests;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 05/07/2014.
 */
public class WeakerAR_Accepted extends PaxosAcceptorMessage {
    private String source;

    public WeakerAR_Accepted( long entityId, int cellNumber, PaxosProposer proposer, String source, String destination) {
        super(proposer, destination,entityId,cellNumber);
        this.source=source;
    }

    @Override
    public void act(String[] messageParts) {
        String source=messageParts[3];
        if(proposer!=null)
            proposer.addToValueAcceptorsList(source);
    }

    @Override
    public String getID() {
        return "WeakerARAccepted";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + source;
    }
}