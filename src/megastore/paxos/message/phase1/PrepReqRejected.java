package megastore.paxos.message.phase1;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 02/05/2014.
 */
public class PrepReqRejected extends PaxosAcceptorMessage {
    private String sourceURL;
    private int proposalNumber;

    public PrepReqRejected( long entityId, int cellNumber, PaxosProposer proposer, String source, String destination, int proposalNumber) {
        super(proposer, destination,entityId,cellNumber);
        this.sourceURL=source;
        this.proposalNumber=proposalNumber;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[3];
        int number = Integer.parseInt( messageParts[4] );

        proposer.addProposalRejector(source);
    }

    @Override
    public String getID() {
        return "PrepReqRejected";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+"," + sourceURL+ "," + proposalNumber;
    }
}
