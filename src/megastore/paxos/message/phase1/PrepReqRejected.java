package megastore.paxos.message.phase1;

import megastore.paxos.Paxos;
import megastore.paxos.message.Message;

/**
 * Created by George on 02/05/2014.
 */
public class PrepReqRejected extends Message {
    private String sourceURL;
    private int proposalNumber;

    public PrepReqRejected(Paxos paxos, String source, String destination, int proposalNumber) {
        super(paxos, destination);
        this.sourceURL=source;
        this.proposalNumber=proposalNumber;
    }

    @Override
    public void act(String[] messageParts) {
        // we try again proposing but now we updated the proposal number that would work
        String source = messageParts[1];
        int number = Integer.parseInt( messageParts[2] );

        paxos.proposer.cleanProposalAcceptorsList();
        if(paxos.proposer.getHighestAcceptedNumber() < number)
            paxos.proposer.setHighestAcceptedNumber(number);

        // we update the proposal thread that at leas one accept proposal failed
        // (so that he can start making prepare proposals again)
        paxos.prepareRequestsDidNotSucceded();
    }

    @Override
    public String getID() {
        return "PrepReqRejected";
    }

    @Override
    protected String toMessage() {
        return getID()+"," + sourceURL+ "," + proposalNumber;
    }
}
