package megastore.paxos.message.phase2;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 03/05/2014.
 */
public class AR_Rejected  extends PaxosAcceptorMessage {
    private String source;
    private int pNumber;

    public AR_Rejected( long entityId, int cellNumber, PaxosProposer proposer, String source, String destination, int pNumber) {
        super(proposer, destination,entityId,cellNumber);
        this.source=source;
        this.pNumber=pNumber;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[4];
        int number = Integer.parseInt( messageParts[3] );

        proposer.increaseValueRejectorsNr();
    }

    @Override
    public String getID() {
        return "AR_Rejected";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + pNumber + "," + source;
    }
}
