package megastore.network;

import megastore.network.message.NetworkMessage;

import java.util.LinkedList;

/**
 * Created by George on 21/06/2014.
 */
public class MessageResponderThread implements Runnable {

    private Thread worker;
    private final LinkedList<NetworkMessage> priorityQueue;
    private final LinkedList<String[]> inputs;

    public MessageResponderThread() {
        priorityQueue = new LinkedList<NetworkMessage>();
        inputs=new LinkedList<String[]>();
        worker =new Thread(this);
    }

    public void addInFront(NetworkMessage n, String[] input) {
        synchronized (priorityQueue) {
            priorityQueue.push(n);
            inputs.push(input);
        }
/*        synchronized (priorityQueue) {
            priorityQueue.add(n);
            inputs.add(input);
        }*/

        startThreadIfNotAlive();
    }

    public void addBehind(NetworkMessage n, String[] input) {
        synchronized (priorityQueue) {
            priorityQueue.add(n);
            inputs.add(input);
        }

        startThreadIfNotAlive();
    }

    private synchronized void startThreadIfNotAlive() {
        if (!worker.isAlive()) {
            worker=new Thread(this);
            worker.start();
        }
    }

    @Override
    public void run() {
        while(true) {
            NetworkMessage message=null;
            String[] input=null;

            synchronized (priorityQueue) {
                if (priorityQueue.isEmpty())
                    return;
                message = priorityQueue.poll();
                input = inputs.poll();
            }

            message.act(input);
        }
    }
}
