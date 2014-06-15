package megastore.paxos.message.phase2;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.paxos.proposer.Proposal;

/**
 * Created by George on 02/05/2014.
 */
public class AcceptRequest extends PaxosProposerMessage {
    private String sourceURL;
    private Proposal proposal;

    public AcceptRequest( long entityId, int cellNumber, NetworkManager networkManager,
                          String sourceURL, String destinationURL, Proposal proposal) {
        super(networkManager,destinationURL,entityId,cellNumber);
        this.proposal =proposal;
        this.sourceURL=sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        //    (b) If an acceptor receives an accept request for a proposal numbered n,
        entityId=Long.parseLong( messageParts[0] );
        cellNumber=Integer.parseInt( messageParts[1] );

        Proposal prop = new Proposal( messageParts[3] );
        String source= messageParts[4];

        synchronized (networkManager) {
            // unless it has already responded to a prepare request having a number greater than n.
            // and we didn't used that log position
            if ((!networkManager.isLogPosOccupied(entityId, cellNumber)) &&
                    //TODO make a way for him to realize that
                    (acceptor.getHighestPropNumberAcc() < prop.pNumber)) {
                // it accepts the proposal
                acceptor.setHighestPropAcc(prop);
                acceptor.setHighestPropNumberAcc(prop.pNumber);
                networkManager.writeValueOnLog(entityId, cellNumber, prop.value); //we also set the final value
                new AR_Accepted(entityId, cellNumber, null, networkManager.getCurrentUrl(), source, prop.pNumber).send();
            } else {
                // it sends a denial message
                new AR_Rejected(entityId, cellNumber, null, networkManager.getCurrentUrl(), source, prop.pNumber).send();
            }
        }
    }

    @Override
    public String getID() {
        return "AcceptRequest";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID()+ "," + proposal.toMessage() + "," + sourceURL;
    }
}
