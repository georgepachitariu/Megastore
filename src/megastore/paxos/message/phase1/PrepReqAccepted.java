package megastore.paxos.message.phase1;

import megastore.paxos.Paxos;
import megastore.paxos.message.Message;

public class PrepReqAccepted extends Message {
    private int propNumber;
    private String sourceURL;

    public PrepReqAccepted(Paxos paxos, String sourceURL, String destinationURL, int propNumber) {
        super(paxos, destinationURL);
        this.propNumber=propNumber;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[1];
        int number = Integer.parseInt( messageParts[2] );

        paxos.proposer.addNodeAsAcceptorOfProposal(source);
        if(paxos.proposer.getHighestAcceptedNumber() < number)
            paxos.proposer.setHighestAcceptedNumber(number);
    }

    @Override
    protected String toMessage() {
            return getID()+"," + sourceURL+ ","  + propNumber;
    }

    @Override
    public String getID() {
        return "PrepReqAccepted";
    }
}
