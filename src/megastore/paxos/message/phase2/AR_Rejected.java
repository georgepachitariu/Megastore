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
