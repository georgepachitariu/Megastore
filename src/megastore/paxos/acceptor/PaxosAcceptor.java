package megastore.paxos.acceptor;

import megastore.paxos.proposer.Proposal;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosAcceptor {

    private long entityId;
    private int cellNumber;
    private Proposal highestPropAcc;
    private int highestPropNumberAcc;

    public PaxosAcceptor( long entityId, int cellNumber) {
        highestPropAcc=null;
        highestPropNumberAcc=-1;
        this.entityId=entityId;
        this.cellNumber=cellNumber;
    }

    public synchronized void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }

    public synchronized void setHighestPropNumberAcc(int highestPropNumberAcc) {
        this.highestPropNumberAcc = highestPropNumberAcc;
    }

    public int getHighestPropNumberAcc() {
        return highestPropNumberAcc;
    }

    public void cleanUp() {
        highestPropAcc=null;
        highestPropNumberAcc=-1;
    }

    public boolean isTheRightSession(long entityId, int cellNumber) {
        return (entityId == this.entityId) && (cellNumber == this.cellNumber);
    }
}
