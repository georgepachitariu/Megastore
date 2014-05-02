package megastore.paxos.message;

import megastore.paxos.Paxos;

public class PrepReqAccepted extends Message {
    public String sourceURL;
    public int paxosRound;
    public Object value;

    public PrepReqAccepted(Paxos paxos, String sourceURL, String destinationURL, int paxosRound, Object value) {
        super(paxos, destinationURL);
        this.value=value;
        this.sourceURL=sourceURL;
        this.paxosRound=paxosRound;
    }

    @Override
    public void act(String[] messageParts) {
        String source = messageParts[1];
        int round = Integer.parseInt( messageParts[2] );
        String objValue = messageParts[3];

        paxos.setHighestPrepReqAnswered(
                new PrepareRequest(paxos, source, null, round, 0, objValue));
        paxos.addNodeAsAcceptorOfProposal(source);
    }

    @Override
    protected String toMessage() {
        return getID()+"," + sourceURL+ "," + paxosRound + "," + value.toString();
    }

    @Override
    public String getID() {
        return "PrepReqAccepted";
    }
}
