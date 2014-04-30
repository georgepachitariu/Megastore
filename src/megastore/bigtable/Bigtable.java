package megastore.bigtable;

import java.util.HashMap;

public class Bigtable {
    private HashMap<Long, Object> map;

    public Bigtable() {
        map=new HashMap<Long, Object>();
    }

    public void put(long key, Object value) {
        map.put(key,value);
    }

    public void delete(long key) {
        map.put(key, null);
    }

    public Object get(long key) {
        return map.get(key);
    }
}
