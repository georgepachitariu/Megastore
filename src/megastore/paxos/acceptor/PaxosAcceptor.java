package megastore.paxos.acceptor;

import megastore.network.NetworkManager;
import megastore.paxos.proposer.Proposal;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;

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

    public Proposal getHighestAcceptedProposal(NetworkManager networkManager, int propNumber ) {
        // this is basically the code from PrepareRequest class

        // returns the highest-numbered proposal (if any) that it has accepted.
        // highestPropAcc may be null
        if(highestPropAcc == null) {
            LogCell cell = networkManager.getMegastore().getEntity(entityId).getLog().get(cellNumber);
            if((cell!=null) && (cell instanceof ValidLogCell))
                highestPropAcc=new Proposal((ValidLogCell)cell,propNumber+1);
        }
        return highestPropAcc;
    }

    public boolean acceptsPrepareProposal(int propNumber) {
        // this is basically the code from PrepareRequest class
        if(highestPropNumberAcc <= propNumber) {
            //promise not to accept any more proposals numbered less than n
            highestPropNumberAcc=propNumber;
            return true;
        }
        else
            return false;
    }
}
