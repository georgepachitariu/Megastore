package experiments;

import megastore.Write;

import java.util.LinkedList;

/**
 * Created by George on 24/06/2014.
 */
public class OperationTimerThread implements Runnable {

    private final double opsPerSecond;
    private final AutomatedDBClient client;
    private final Thread parent;
    private final int startingPoint;

    public OperationTimerThread(AutomatedDBClient client, double opsPerSecond, Thread parent, int startingPoint) {
        this.opsPerSecond=opsPerSecond;
        this.client = client;
        this.parent=parent;
        this.startingPoint=startingPoint;
    }

    @Override
    public void run() {
        if (new Double(0).equals( opsPerSecond ))
            return;
        double timeToWait = 1000 / opsPerSecond;


        int i=0;
        while (parent.isAlive()) {
            try {
                Thread.sleep((int)timeToWait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String key = String.valueOf(startingPoint + i);
            String newValue = String.valueOf(startingPoint + i);

            LinkedList<Write> list=new LinkedList<Write>();
            list.add(new Write(key,newValue));

            client.addOperation(list);
            i++;
        }
    }
}
