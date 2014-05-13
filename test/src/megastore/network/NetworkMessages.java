package megastore.network;

import megastore.Entity;
import megastore.Megastore;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by George on 11/05/2014.
 */
public class NetworkMessages {
    @Test
    public void introduction_availableNodesTest() {
        NetworkManager n1=new NetworkManager(null, "61616");
        NetworkManager n2=new NetworkManager(null, "61617");

        n2.updateNodesFrom("192.168.1.100:61616");
        List<String> list = n2.getNodesURL();
        Assert.assertTrue(list.size() ==2 &&
                list.get(0).equals("192.168.1.100:61616")  &&
                list.get(1).equals("192.168.1.100:61617")  );

        n1.close();
        n2.close();
    }

    @Test
    public void createEntity_putValueTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617","192.168.1.100:61616");

        Entity e=m1.createEntity();
        if(! e.put("white","cat"))
            System.out.println("Put command failed");
        else {
            LogCell cell = m2.getExistingEntities().get(0).getLog().get(0);
            Assert.assertTrue(e.getHashValue("white") == ((ValidLogCell) cell).logList.get(0).key);
            Assert.assertTrue(((ValidLogCell) cell).logList.get(0).newValue.equals("cat"));
            Assert.assertTrue(((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(0)).logList.get(0).newValue.equals("cat"));
        }

        m1.close();
        m2.close();
    }

    @Test
    public void createEntity_putValue_getKeyTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617","192.168.1.100:61616");

        Entity e1=m1.createEntity();
        if(! e1.put("white","cat"))
            System.out.println("Put command failed");

        Entity e2 = m2.getEntity(e1.getEntityID());
        LogCell result=e2.get("white");

        m1.close();
        m2.close();
    }
}
