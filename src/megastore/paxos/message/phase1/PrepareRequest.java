package megastore.paxos.message.phase1;

import megastore.paxos.Paxos;
import megastore.paxos.proposer.Proposal;
import megastore.paxos.message.Message;

public class PrepareRequest extends Message {
    private String sourceURL;
    private int proposalNumber;

    public PrepareRequest(Paxos paxos, String sourceURL, String destinationURL, int proposalNumber) {
        super(paxos, destinationURL);
        this.proposalNumber=proposalNumber;
        this.sourceURL = sourceURL;
    }

    public void act(String[] messageParts) {
        //            If an acceptor receives a prepare request with number n greater than
        //            that of any prepare request to which it has already responded,
        int propNumber = Integer.parseInt(messageParts[2]);
        String sourceL=messageParts[1];

        Proposal highestPropAcc = paxos.acceptor.getHighestPropAcc();
        int highestPropNumberAcc = paxos.acceptor.getHighestPropNumberAcc();

        if(highestPropNumberAcc <= propNumber) {
            //promise not to accept any more proposals numbered less than n
            paxos.acceptor.setHighestPropNumberAcc(propNumber);

            // and with the highest-numbered proposal (if any) that it has accepted.
            // highestPropAcc may be null
            if(highestPropAcc != null) {
                new PrepReqAcceptedWithProp(paxos, paxos.getCurrentUrl(),
                        sourceL, highestPropAcc).send();
            }
            else {
                new PrepReqAccepted(paxos, paxos.getCurrentUrl(),sourceL).send();
            }
        }
        else {
            new PrepReqRejected(paxos, paxos.getCurrentUrl(), sourceL, highestPropNumberAcc).send();
        }
    }

    public String toMessage() {
        return getID()+"," + sourceURL + "," + proposalNumber;
    }

    @Override
    public String getID() {
        return "PrepareRequest";
    }
}
