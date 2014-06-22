package megastore.paxos.message.phase1;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

public class PrepReqAccepted extends PaxosAcceptorMessage {
    private String sourceURL;

    public PrepReqAccepted( long entityId, int cellNumber, PaxosProposer proposer, String sourceURL, String destinationURL) {
        super(proposer, destinationURL,entityId,cellNumber);
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[3];

        proposer.addPrepareRequestAcceptor(source);
    }

    @Override
    protected String toMessage() {
            return super.toMessage() + getID()+"," + sourceURL;
    }

    @Override
    public String getID() {
        return "PrepReqAccepted";
    }
}
