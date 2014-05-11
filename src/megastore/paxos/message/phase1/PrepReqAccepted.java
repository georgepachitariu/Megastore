package megastore.paxos.message.phase1;

import megastore.paxos.Paxos;
import megastore.paxos.message.Message;

public class PrepReqAccepted extends Message {
    private String sourceURL;

    public PrepReqAccepted(Paxos paxos, String sourceURL, String destinationURL) {
        super(paxos, destinationURL);
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[1];

        paxos.proposer.addNodeAsAcceptorOfProposal(source);
    }

    @Override
    protected String toMessage() {
            return getID()+"," + sourceURL;
    }

    @Override
    public String getID() {
        return "PrepReqAccepted";
    }
}
