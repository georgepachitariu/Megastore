package megastore.network;

import megastore.Entity;
import megastore.Megastore;
import megastore.write_ahead_log.LogCell;
import megastore.write_ahead_log.ValidLogCell;
import megastore.write_ahead_log.WriteOperation;
import org.junit.Assert;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by George on 11/05/2014.
 */
public class NetworkMessages {
    static String  ip () {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void introduction_availableNodesTest() {
        NetworkManager n1=new NetworkManager(null, "61616");
        NetworkManager n2=new NetworkManager(null, "61617");

        n2.updateNodesFrom(ip()+":61616");
        List<String> list = n2.getNodesURL();
        Assert.assertTrue(list.size() ==2 &&
                list.get(0).equals(ip()+":61616")  &&
                list.get(1).equals(ip()+":61617")  );

        n1.close();
        n2.close();
    }

    @Test
    public void createEntity_putValueTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617",ip()+":61616");
        Megastore m3=new Megastore("61618",ip()+":61616");


        Entity e=m1.createEntity();
        if(! e.put("white", "cat"))
            System.out.println("Put command failed");
        else {
            WriteOperation expected = new WriteOperation(e.getHashValue("white"), "cat");
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(0)).logList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(0)).logList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(0)).logList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }


        if(! m2.getExistingEntities().get(0).put("black", "dog"))
            System.out.println("Put command failed");
        else {
            WriteOperation expected = new WriteOperation(e.getHashValue("black"), "dog");
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(1)).logList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(1)).logList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(1)).logList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }

        if(! m3.getExistingEntities().get(0).put("green", "boogie"))
            System.out.println("Put command failed");
        else {
            WriteOperation expected = new WriteOperation(e.getHashValue("green"), "boogie");
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(2)).logList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(2)).logList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(2)).logList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }

        m1.close();
        m2.close();
        m3.close();
    }

    @Test
    public void createEntity_putValue_getKeyTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617",ip()+":61616");

        Entity e1=m1.createEntity();
        if(! e1.put("white","cat"))
            System.out.println("Put command failed");

        Entity e2 = m2.getEntity(e1.getEntityID());
        LogCell result=e2.get("white");

        m1.close();
        m2.close();
    }
}
