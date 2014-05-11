package megastore.paxos.acceptor;

import megastore.paxos.proposer.Proposal;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosAcceptor {
    private Proposal highestPropAcc;
    private int highestPropNumberAcc;

    public PaxosAcceptor() {
        highestPropAcc=null;
        highestPropNumberAcc=-1;
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
}
