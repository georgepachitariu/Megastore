package megastore.coordinator;

import megastore.LogBuffer;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by George on 13/05/2014.
 */
public class Coordinator {
//    A coordinator server tracks a set of entity groups for which its replica
//    has observed all Paxos writes. For entity groups in that set, the replica has
//    suficient state to serve local reads.
    private List<EntityState> entities;

    public Coordinator() {
        entities=new LinkedList<EntityState>();
    }

    public void addEntity (long entityID) {
        entities.add(new EntityState(entityID, true));
    }

    public void invalidate(long entityID) {
//        If a write is not accepted on a replica, we must remove the entity group 's
//        key from that replica 's coordinator. This process is called invalidation.
        for(EntityState e : entities)
            if(e.entityID == entityID)
                e.isValid=false;
    }

    public boolean isUpToDate(long entityID) {
        for(EntityState e : entities)
            if(e.entityID == entityID)
                return e.isValid;
        LogBuffer.println("EntityID not found");
        return false;
    }

    public void validate(long entityID) {
        for(EntityState e : entities)
            if(e.entityID == entityID)
                e.isValid=true;
    }
}
