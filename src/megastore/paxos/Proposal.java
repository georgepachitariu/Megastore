package megastore.paxos;

public class Proposal {
    public Object value;
    public int pNumber;

    public Proposal(Object value, int pNumber) {
        this.value = value;
        this.pNumber = pNumber;
    }

    public Proposal(String raw) {
        pNumber = Integer.parseInt( raw.split("/")[0] );
        value = raw.split("/")[1];
    }

    public String toMessage() {
        return pNumber +"/" + value.toString();
    }
}
