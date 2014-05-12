package megastore.paxos.message;

import megastore.network.NetworkManager;
import megastore.network.message.NetworkMessage;
import megastore.paxos.acceptor.PaxosAcceptor;

/**
 * Created by George on 12/05/2014.
 */
public abstract class PaxosProposerMessage extends NetworkMessage {

        protected long entityId;
        protected int cellNumber;
        protected PaxosAcceptor acceptor;
        protected NetworkManager networkManager;

        protected PaxosProposerMessage(NetworkManager networkManager,
                                       String destinationURL, long entityId, int cellNumber) {
            super(destinationURL);
            this.networkManager=networkManager;
        }

        public abstract void act(String[] messageParts);
        public abstract String getID();
        protected String toMessage() {
            return entityId+","+cellNumber+ ",";
        }

        public void setAcceptor(PaxosAcceptor acceptor) {
            this.acceptor = acceptor;
        }
}

