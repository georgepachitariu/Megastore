package megastore.paxos.message.phase2;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 20/06/2014.
 */
public class EnforcedAR_Rejected  extends PaxosAcceptorMessage {
    private String source;

    public EnforcedAR_Rejected( long entityId, int cellNumber, PaxosProposer proposer, String source, String destination) {
        super(proposer, destination,entityId,cellNumber);
        this.source=source;
    }

    @Override
    public void act(String[] messageParts) {
        String source=messageParts[3];
        if(proposer!=null)
            proposer.addToValueRejectorsList(source);
    }

    @Override
    public String getID() {
        return "EnforcedARRejected";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + source;
    }
}