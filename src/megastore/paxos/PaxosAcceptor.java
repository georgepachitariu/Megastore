package megastore.paxos;

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

    public void setHighestPropAcc(Proposal highestPropAcc) {
        this.highestPropAcc = highestPropAcc;
    }

    public Proposal getHighestPropAcc() {
        return highestPropAcc;
    }

    public void setHighestPropNumberAcc(int highestPropNumberAcc) {
        this.highestPropNumberAcc = highestPropNumberAcc;
    }

    public int getHighestPropNumberAcc() {
        return highestPropNumberAcc;
    }

}
