package megastore.paxos;

public class Proposal {
    public String sourceURL;
    public Object value;
    public int nr;

    public Proposal(String sourceURL, int nr, Object value) {
        this.nr = nr;
        this.value=value;
        this.sourceURL=sourceURL;
    }

    @Override
    public String toString() {
        return "P" +nr+"," + sourceURL + "," + value.toString();
    }
}
