package megastore.paxos.proposer;

import megastore.write_ahead_log.ValidLogCell;

public class Proposal {
    public ValidLogCell value;
    public int pNumber;

    public Proposal(ValidLogCell value, int pNumber) {
        this.value = value;
        this.pNumber = pNumber;
    }

    public Proposal(String raw) {
        pNumber = Integer.parseInt( raw.split("/")[0] );
        value = new ValidLogCell(raw.split("/")[1]);
    }

    public String toMessage() {
        return pNumber +"/" + value.toString();
    }
}
