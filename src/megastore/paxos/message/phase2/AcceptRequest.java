package megastore.paxos.message.phase2;

import megastore.paxos.Paxos;
import megastore.paxos.proposer.Proposal;
import megastore.paxos.message.Message;

/**
 * Created by George on 02/05/2014.
 */
public class AcceptRequest extends Message {
    private String sourceURL;
    private Proposal proposal;

    public AcceptRequest(Paxos paxos, String sourceURL, String destinationURL, Proposal proposal) {
        super(paxos, destinationURL);
        this.proposal =proposal;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        //    (b) If an acceptor receives an accept request for a proposal numbered n,
        Proposal prop = new Proposal( messageParts[1] );
        String source= messageParts[2];

        // unless it has already responded to a prepare request having a number greater than n.
        if(paxos.acceptor.getHighestPropNumberAcc() <=  prop.pNumber) {
            // it accepts the proposal
            paxos.acceptor.setHighestPropAcc(prop);
            paxos.acceptor.setHighestPropNumberAcc(prop.pNumber);
            paxos.setFinalValue(prop.value); //we also set the final value
            new AR_Accepted(paxos, paxos.getCurrentUrl(), source,  prop.pNumber).send();
       }
        else {
            // it sends a denial message
            new AR_Rejected(paxos, paxos.getCurrentUrl(), source,  prop.pNumber).send();
        }
    }

    @Override
    public String getID() {
      return "AcceptRequest";
    }

    @Override
    protected String toMessage() {
        return getID()+ "," + proposal.toMessage() + "," + sourceURL;
    }
}
