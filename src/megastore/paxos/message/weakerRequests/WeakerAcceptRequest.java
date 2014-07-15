package megastore.paxos.message.weakerRequests;

import megastore.network.NetworkManager;
import megastore.paxos.message.PaxosProposerMessage;
import megastore.write_ahead_log.ValidLogCell;

/**
 * Created by George on 05/07/2014.
 */
public class WeakerAcceptRequest extends PaxosProposerMessage {
    private String sourceURL;
    private ValidLogCell value;

    public WeakerAcceptRequest(long entityId, int cellNumber, NetworkManager networkManager,
                               String sourceURL, String destinationURL, ValidLogCell value) {
        super(networkManager, destinationURL, entityId, cellNumber);
        this.value = value;
        this.sourceURL = sourceURL;
    }

    @Override
    public void act(String[] messageParts) {
        entityId = Long.parseLong(messageParts[0]);
        cellNumber = Integer.parseInt(messageParts[1]);

        ValidLogCell value = new ValidLogCell(messageParts[3]);
        String source = messageParts[4];
        int maxim=10;
        while(maxim>0) {
             if(  networkManager.isLogPosOpenedForWeakerProposals(entityId, cellNumber))
                 break;
            maxim-=2;
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (networkManager.isLogPosOpenedForWeakerProposals(entityId, cellNumber)) {
            networkManager.writeValueOnLog(entityId, cellNumber, value); //we also set the final value
            new WeakerAR_Accepted(entityId, cellNumber, null, networkManager.getCurrentUrl(), source).send();
            networkManager.getMegastore().getEntity(entityId).
                    makeCellOpenedForWeakProposals(cellNumber+1);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    networkManager.getMegastore().getEntity(entityId).
                            proposeValueToLeaderAgainIfThereWasOne(entityId, cellNumber);
                }
            }).start();

        }
        else
            new WeakerAR_Rejected(entityId, cellNumber, null, networkManager.getCurrentUrl(), source).send();
    }


    @Override
    public String getID() {
        return "WeakerAcceptRequest";
    }

    @Override
    protected String toMessage() {
        return super.toMessage() + getID() + "," + value.toString() + "," + sourceURL;
    }
}