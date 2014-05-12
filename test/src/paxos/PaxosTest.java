package paxos;

/**
 * Created by George on 03/05/2014.
 */
public class PaxosTest {

    /*
    @Test
    public void stressTestPaxos() throws InterruptedException {
        String url1="192.168.1.100:61616";
        String url2="192.168.1.100:61617";
        String url3="192.168.1.100:61618";
        String url4="192.168.1.100:61619";
        LinkedList<String> list =new LinkedList<String>();
        list.add(url1);
        list.add(url2);
        list.add(url3);
        list.add(url4);

        Paxos p1=new Paxos("61616", list,"1");
        Paxos p2=new Paxos("61617", list,"2");
        Paxos p3=new Paxos("61618", list,"3");
        Paxos p4=new Paxos("61619", list,"4");

        p1.startThread();
        p2.startThread();
        p3.startThread();
        p4.startThread();

        Queue<String> p1_list=getRandomList(75);
        Queue<String> p2_list=getRandomList(75);
        Queue<String> p3_list=getRandomList(75);
        Queue<String> p4_list=getRandomList(75);

        int k=0;
        long before=System.currentTimeMillis();
        while(p1_list.size()+p2_list.size()+p3_list.size()+p4_list.size()>0) {

            Thread thr1 = new Thread(p1, "thr1");
            Thread thr2 = new Thread(p2, "thr2");
            Thread thr3 = new Thread(p3, "thr3");
            Thread thr4 = new Thread(p4, "thr4");

            String n1="",n2="",n3="",n4="";
            if(p1_list.size()>0) {
                n1=p1_list.peek();
                p1.setValue(n1);
                thr1.start();
            }
            if(p2_list.size()>0) {
                n2=p2_list.peek();
                p2.setValue(n2);
                thr2.start();
            }
            if(p3_list.size()>0) {
                n3=p3_list.peek();
                p3.setValue(n3);
                thr3.start();
            }
            if(p4_list.size()>0) {
                n4=p4_list.peek();
                p4.setValue(n4);
                thr4.start();
            }

            thr1.join();
            thr2.join();
            thr3.join();
            thr4.join();

            if(p1.succeeded() || p2.succeeded() || p3.succeeded() || p4.succeeded())
                Assert.assertTrue(p1.getFinalValue().equals(p2.getFinalValue()) &&
                        p2.getFinalValue().equals(p3.getFinalValue()) &&
                        p3.getFinalValue().equals(p4.getFinalValue()));
            else
                System.out.println("Nobody won :( ");

            int victoriousThread = 0;
            if (p1.getFinalValue() !=null && p1.getFinalValue().equals(n1)) {
                victoriousThread = 1;
                p1_list.poll();
            }
            if (p2.getFinalValue() !=null && p2.getFinalValue().equals(n2)) {
                victoriousThread = 2;
                p2_list.poll();
            }
            if (p3.getFinalValue() !=null && p3.getFinalValue().equals(n3)) {
                victoriousThread = 3;
                p3_list.poll();
            }
            if (p4.getFinalValue() !=null && p4.getFinalValue().equals(n4)) {
                victoriousThread = 4;
                p4_list.poll();
            }
             //   System.out.println(p1.getFinalValue() + " . And the thread who won is -- " + victoriousThread);

            p1.cleanUp();
            p2.cleanUp();
            p3.cleanUp();
            p4.cleanUp();

            if(++k == 70) {
                k=0;
                long after=System.currentTimeMillis();
                System.out.println(after - before + " the time it took in milliseconds for 70 operations");
                before=System.currentTimeMillis();
            }
        }

        p1.close();
        p2.close();
        p3.close();
        p4.close();
    }

    private Queue<String> getRandomList(int i) {
        Queue<String> list=new LinkedList<String>();
        Random rand = new Random();
        for(;i>0; i--) {
            int nr=rand.nextInt(1000);
            list.add(String.valueOf(nr));
        }
        return  list;
    } */
}

