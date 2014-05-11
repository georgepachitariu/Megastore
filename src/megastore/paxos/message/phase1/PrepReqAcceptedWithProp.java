package megastore.paxos.message.phase1;

import megastore.paxos.Paxos;
import megastore.paxos.proposer.Proposal;
import megastore.paxos.message.Message;

public class PrepReqAcceptedWithProp extends Message {
    private Proposal proposal;
    private String sourceURL;

    public PrepReqAcceptedWithProp(Paxos paxos, String sourceURL, String destinationURL, Proposal proposal) {
        super(paxos, destinationURL);
        this.proposal=proposal;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[1];
        Proposal prop = new Proposal(messageParts[2]);

        Proposal oldP = paxos.proposer.getHighestPropAcc();
        if(oldP==null || oldP.pNumber < prop.pNumber ) {
            paxos.proposer.setHighestPropAcc( prop );
        }

        paxos.proposer.addNodeAsAcceptorOfProposal(source);
    }

    @Override
    protected String toMessage() {
            return getID()+"," + sourceURL+ "," + proposal.toMessage();
    }

    @Override
    public String getID() {
        return "PrepReqAcceptedWithProp";
    }
}
