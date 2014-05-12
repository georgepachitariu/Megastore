package megastore.paxos.proposer;

import megastore.write_ahead_log.LogCell;

public class Proposal {
    public LogCell value;
    public int pNumber;

    public Proposal(LogCell value, int pNumber) {
        this.value = value;
        this.pNumber = pNumber;
    }

    public Proposal(String raw) {
        pNumber = Integer.parseInt( raw.split("/")[0] );
        value = new LogCell(raw.split("/")[1]);
    }

    public String toMessage() {
        return pNumber +"/" + value.toString();
    }
}
