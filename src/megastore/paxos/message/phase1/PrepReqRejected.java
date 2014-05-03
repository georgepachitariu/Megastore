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
        System.out.println( "Got REJECTEEEED from " + messageParts[1]);
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
