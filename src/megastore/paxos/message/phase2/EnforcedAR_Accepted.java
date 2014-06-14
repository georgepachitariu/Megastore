package megastore.paxos.message.phase2;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 13/05/2014.
 */
public class EnforcedAR_Accepted extends PaxosAcceptorMessage {
    private String source;

    public EnforcedAR_Accepted( long entityId, int cellNumber, PaxosProposer proposer, String source, String destination) {
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
        return "EnforcedARAccepted";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + source;
    }
}