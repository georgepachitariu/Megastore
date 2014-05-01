package megastore.paxos;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.List;

public class Paxos {
    private List<String> nodesURL;
    private ListeningThread listeningThread;

    public Paxos(String port, List<String> nodesURL) {
        // a listeningThread that is listening for proposals or accept proposals
        listeningThread =new ListeningThread(port);
        Thread runThread = new Thread(this.listeningThread);
        runThread .setDaemon(false);
        runThread .start();

        this.nodesURL = nodesURL;
    }

    public Paxos(String port, List<String> nodesURL, ListeningThread thread) {
        Thread runThread  = new Thread(thread);
        runThread .setDaemon(false);
        runThread .start();

        this.listeningThread = thread;
        this.nodesURL = nodesURL;
    }

    // to be called by megastore after a write operation
    // has been made local
    public void proposeValue(Object value) {
        // Phase 1. (a) A proposer selects a proposal number n and sends a prepare
        // request with number n to a majority of acceptors.
        for(String s : nodesURL)
            if(listeningThread.getCurrentUrl()!=s)
                sendMessage(s,
                    new Proposal(listeningThread.getCurrentUrl(),  1,   value). toString()
            );
    }

    public void sendMessage(String nodeUrl, String message) {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(nodeUrl);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue("TEST.FOO");

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Create a message
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);

        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
}