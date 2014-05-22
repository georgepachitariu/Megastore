package megastore.network;

import megastore.Entity;
import megastore.Megastore;
import megastore.write_ahead_log.Log;
import megastore.write_ahead_log.UnacceptedLogCell;
import megastore.write_ahead_log.ValidLogCell;
import megastore.write_ahead_log.WriteOperation;
import org.junit.Assert;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.LinkedList;
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
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(0)).opList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(0)).opList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(0)).opList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }


        if(! m2.getExistingEntities().get(0).put("black", "dog"))
            System.out.println("Put command failed");
        else {
            WriteOperation expected = new WriteOperation(e.getHashValue("black"), "dog");
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(1)).opList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(1)).opList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(1)).opList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }

        if(! m3.getExistingEntities().get(0).put("green", "boogie"))
            System.out.println("Put command failed");
        else {
            WriteOperation expected = new WriteOperation(e.getHashValue("green"), "boogie");
            WriteOperation cell1 = ((ValidLogCell) m1.getExistingEntities().get(0).getLog().get(2)).opList.get(0);
            WriteOperation cell2 = ((ValidLogCell) m2.getExistingEntities().get(0).getLog().get(2)).opList.get(0);
            WriteOperation cell3 = ((ValidLogCell) m3.getExistingEntities().get(0).getLog().get(2)).opList.get(0);

            Assert.assertTrue(expected.equals(cell1));
            Assert.assertTrue(expected.equals(cell2));
            Assert.assertTrue(expected.equals(cell3));
        }

        m1.close();
        m2.close();
        m3.close();
    }

    @Test
    public void createEntity_put_getTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617",ip()+":61616");
        Megastore m3=new Megastore("61618",ip()+":61616");

        Entity e=m1.createEntity();
        if(! e.put("white", "cat"))
            System.out.println("Put command failed");
        else {
            String actual1 = m1.getEntity (e.getEntityID()).get("white");
            String actual2 = m2.getEntity (e.getEntityID()).get("white");
            String actual3 = m3.getEntity (e.getEntityID()).get("white");

            Assert.assertTrue("cat".equals(actual1));
            Assert.assertTrue("cat".equals(actual2));
            Assert.assertTrue("cat".equals(actual3));
        }

        m1.close();
        m2.close();
        m3.close();
    }

    @Test
    public void create2Entities_put_getTest() {
        Megastore m1=new Megastore("61616");
        Megastore m2=new Megastore("61617",ip()+":61616");
        Megastore m3=new Megastore("61618",ip()+":61616");

        Entity e=m1.createEntity();
        Entity e2=m2.createEntity();
        if(! ( e2.put("white", "cat") && e.put("white", "jaguar")) )
            System.out.println("Put command failed");
        else {
            String actual1 = m1.getEntity (e2.getEntityID()).get("white");
            String actual3 = m3.getEntity (e.getEntityID()).get("white");

            Assert.assertTrue("cat".equals(actual1));
            Assert.assertTrue("jaguar".equals(actual3));
        }

        m1.close();
        m2.close();
        m3.close();
    }

    List<WriteOperation> getWriteListWith(long nr, String str) {
        List<WriteOperation> l= new LinkedList<WriteOperation>();
        l.add(new WriteOperation(nr,str));
        return l;
    }

    @Test
    public void readOp_catchUpTest() {
        // we make two megastores: A and B
        // we want to stress B to catchUp from A and then return;

        String urlA=ip()+":61616";
        String urlB=ip()+":61617";
        List<String> urls=new LinkedList<String>();
        urls.add(urlA);
        urls.add(urlB);

        // A contains two log cells with values
        Log logA=new Log();
        logA.append(new ValidLogCell(urlA,getWriteListWith(5922/*hash("white")*/,"cat")),0);
        logA.append(new ValidLogCell(urlA,getWriteListWith(2345L,"dog")),1);
        Entity e1=new Entity(urls,null,0,logA);
        Megastore A=new Megastore("61616",getBoxedInaList(e1));
        e1.setMegastore(A);
        A.getCoordinator().addEntity(0);

        // B contains only a invalid cell.
        Log logB=new Log();
        logB.append(new UnacceptedLogCell(urlA),0);
        Entity e2=new Entity(urls,null,0,logB);
        Megastore B=new Megastore("61617",urlA, getBoxedInaList(e2));
        e2.setMegastore(B);
        B.getCoordinator().addEntity(0);
        B.getCoordinator().invalidate(0); // we invalidate it's coordinator


        // We want to get last value from B.
        String result=B.getEntity(0).get("white");
        Assert.assertTrue(result.equals("cat"));
    }

    private List<Entity> getBoxedInaList(Entity e1) {
        List<Entity> l=new LinkedList<Entity>();
        l.add(e1);
        return l;
    }

}
