package megastore.paxos.message;

import megastore.network.message.NetworkMessage;
import megastore.paxos.proposer.PaxosProposer;

/**
 * Created by George on 01/05/2014.
 */
public abstract class PaxosAcceptorMessage extends NetworkMessage {

    protected PaxosProposer proposer;
    protected long entityId;
    protected int cellNumber;

    protected PaxosAcceptorMessage(PaxosProposer proposer, String destinationURL, long entityId, int cellNumber) {
        super(destinationURL);
        this.proposer = proposer;
    }

    public abstract void act(String[] messageParts);
    public abstract String getID();
    protected String toMessage() {
        return entityId+","+cellNumber+ ",";
    }

    public void setProposer(PaxosProposer proposer) {
        this.proposer = proposer;
    }
}
