package megastore.paxos.message;

import megastore.paxos.Paxos;

public class PrepareRequest extends Message {
    public String sourceURL;
    public Object value;
    public int proposalNumber;
    public int paxosRound;

    public PrepareRequest(Paxos paxos, String sourceURL, String destinationURL, int paxosRound, int proposalNumber, Object value) {
        super(paxos, destinationURL);
        this.paxosRound=paxosRound;
        this.proposalNumber = proposalNumber;
        this.value=value;
        this.sourceURL=sourceURL;
    }


    public void act(String[] messageParts) {
        //            If an acceptor receives a prepare request with number n greater than
        //            that of any prepare request to which it has already responded,
        int round= Integer.parseInt(messageParts[1]);
        int number= Integer.parseInt(messageParts[2]);
        String sourceL=messageParts[3];
        String value= messageParts[4];

        PrepareRequest highestPrepReqAnswered = paxos.getHighestPrepReqAnswered();

        if(highestPrepReqAnswered ==null) {
            paxos.setHighestPrepReqAnswered(new PrepareRequest(paxos, null, null, round, number, value));
            new PrepReqAccepted(paxos, paxos.getCurrentUrl(), sourceL , round, value).send();
        }
        else if(highestPrepReqAnswered.proposalNumber < number )  {
            // then it responds to the request with a promise not to accept any more proposals numbered less
            // than n and with the highest-numbered proposal (if any) that it has accepted.
            paxos.setProposalNumber(number);
            new PrepReqAccepted(paxos, paxos.getCurrentUrl(), sourceL, round, highestPrepReqAnswered.value).send();
        }
        else {
            new PrepReqRejected(paxos, paxos.getCurrentUrl(), sourceL, highestPrepReqAnswered.proposalNumber).send();
        }
    }

    public String toMessage() {
        return getID()+"," + paxosRound + "," + proposalNumber +"," + sourceURL + "," + value.toString();
    }

    @Override
    public String getID() {
        return "PrepareRequest";
    }
}
