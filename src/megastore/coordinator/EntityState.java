package megastore.coordinator;

/**
 * Created by George on 13/05/2014.
 */
public class EntityState {
    public long entityID;
    public boolean isValid;
    public boolean isTemporaryInvalid;

    public EntityState(long entityID, boolean isValid) {
       this.entityID=entityID;
       this.isValid=isValid;
    }

}
