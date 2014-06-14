package megastore.paxos.message.phase1;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.paxos.proposer.Proposal;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;

public class PrepareRequest extends PaxosProposerMessage {
    private String sourceURL;
    private int proposalNumber;

    public PrepareRequest( long entityId, int cellNumber,NetworkManager networkManager,
                           String sourceURL, String destinationURL, int proposalNumber) {
        super(networkManager, destinationURL,entityId, cellNumber);
        this.proposalNumber=proposalNumber;
        this.sourceURL = sourceURL;
    }

    public void act(String[] messageParts) {
        //            If an acceptor receives a prepare request with number n greater than
        //            that of any prepare request to which it has already responded,
        int propNumber = Integer.parseInt(messageParts[4]);
        String sourceL=messageParts[3];
        entityId=Long.parseLong(messageParts[0]);
        cellNumber=Integer.parseInt(messageParts[1]);

        Proposal highestPropAcc = acceptor.getHighestPropAcc();
        int highestPropNumberAcc = acceptor.getHighestPropNumberAcc();

        if(highestPropNumberAcc <= propNumber) {
            //promise not to accept any more proposals numbered less than n
            acceptor.setHighestPropNumberAcc(propNumber);

            // and with the highest-numbered proposal (if any) that it has accepted.
            // highestPropAcc may be null
            if(highestPropAcc == null) {
                LogCell cell = networkManager.getMegastore().getEntity(entityId).getLog().get(cellNumber);
                if((cell!=null) && (cell instanceof ValidLogCell))
                    highestPropAcc=new Proposal((ValidLogCell)cell,propNumber+1);
            }
            if(highestPropAcc != null) {
                new PrepReqAcceptedWithProp(entityId,cellNumber,null, networkManager.getCurrentUrl(),
                        sourceL, highestPropAcc).send();
            }
            else {
                new PrepReqAccepted(entityId,cellNumber,null, networkManager.getCurrentUrl(),sourceL).send();
            }
        }
        else {
            new PrepReqRejected(entityId,cellNumber,null, networkManager.getCurrentUrl(), sourceL, highestPropNumberAcc).send();
        }
    }

    public String toMessage() {
        return super.toMessage() + getID()+"," + sourceURL + "," + proposalNumber;
    }

    @Override
    public String getID() {
        return "PrepareRequest";
    }
}
