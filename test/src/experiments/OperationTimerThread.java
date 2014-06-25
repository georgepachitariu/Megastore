package experiments;

/**
 * Created by George on 24/06/2014.
 */
public class OperationTimerThread implements Runnable {

    private final double opsPerSecond;
    private final AutomatedDBClient client;
    private final Thread parent;

    public OperationTimerThread(AutomatedDBClient client, double opsPerSecond, Thread parent) {
        this.opsPerSecond=opsPerSecond;
        this.client = client;
        this.parent=parent;
    }

    @Override
    public void run() {
        if (new Double(0).equals( opsPerSecond ))
            return;
        double timeToWait = 1000 / opsPerSecond;

        while (parent.isAlive()) {
            try {
                Thread.sleep((int)timeToWait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            client.addElement(System.currentTimeMillis());
        }
    }
}
