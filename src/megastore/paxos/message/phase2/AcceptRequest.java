package megastore.paxos.message.phase2;

import megastore.paxos.Paxos;
import megastore.paxos.Proposal;
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
