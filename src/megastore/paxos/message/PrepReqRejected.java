package megastore.paxos.message;

import megastore.paxos.Paxos;

/**
 * Created by George on 02/05/2014.
 */
public class PrepReqRejected extends Message {
    public String sourceURL;
    public int proposalNumber;

    public PrepReqRejected(Paxos paxos, String source, String destination, int proposalNumber) {
        super(paxos, destination);
        this.sourceURL=source;
        this.proposalNumber=proposalNumber;
    }

    @Override
    public void act(String[] messageParts) {
        System.out.println("Got REJECTEEEED");
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
