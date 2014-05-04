package megastore.paxos.message.phase2;

import megastore.paxos.Paxos;
import megastore.paxos.message.Message;

/**
 * Created by George on 03/05/2014.
 */
public class AR_Rejected  extends Message {
    private String source;
    private int pNumber;

    public AR_Rejected(Paxos paxos, String source, String destination, int pNumber) {
        super(paxos, destination);
        this.source=source;
        this.pNumber=pNumber;
    }

    @Override
    public void act(String[] messageParts) {
        //this proposal failed. We have to begin again proposing
        // we try again proposing but now we updated the proposal number that would work

        String source = messageParts[2];
        int number = Integer.parseInt( messageParts[1] );

        paxos.proposer.cleanProposalAcceptorsList();
        if(paxos.proposer.getHighestAcceptedNumber() < number)
            paxos.proposer.setHighestAcceptedNumber(number);

        // we update the proposal thread that at leas one accept proposal failed
        // (so that he can start making prepare proposals again)
        paxos.acceptRequestsDidNotSucceded();
    }

    @Override
    public String getID() {
        return "AR_Rejected";
    }

    @Override
    protected String toMessage() {
        return getID()+ "," + pNumber + "," + source;
    }
}
