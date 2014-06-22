package megastore.paxos.message.phase1;

import megastore.paxos.message.PaxosAcceptorMessage;
import megastore.paxos.proposer.PaxosProposer;
import megastore.paxos.proposer.Proposal;

public class PrepReqAcceptedWithProp extends PaxosAcceptorMessage {
    private Proposal proposal;
    private String sourceURL;

    public PrepReqAcceptedWithProp( long entityId, int cellNumber, PaxosProposer proposer, String sourceURL, String destinationURL, Proposal proposal) {
        super(proposer, destinationURL,entityId,cellNumber);
        this.proposal=proposal;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[3];
        Proposal prop = new Proposal(messageParts[4]);

        Proposal oldP = proposer.getHighestPropAcc();
        if(oldP==null || oldP.pNumber < prop.pNumber ) {
            proposer.setHighestPropAcc( prop );
        }

        proposer.addPrepareRequestAcceptor(source);
    }

    @Override
    protected String toMessage() {
            return super.toMessage() + getID()+"," + sourceURL+ "," + proposal.toMessage();
    }

    @Override
    public String getID() {
        return "PrepReqAcceptedWithProp";
    }
}
