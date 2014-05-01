package megastore.paxos;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class ListeningThread  implements Runnable, ExceptionListener {

    private  MessageConsumer consumer;
    private Session session;
    private Connection connection;

    private boolean isAlive;
    private Broker broker;

    private Proposal highestProposalAnswered;

    public ListeningThread(String port) {
        // start the Broker Server first
        broker=new Broker(port);
        Thread brokerThread = new Thread(broker);
        brokerThread.setDaemon(false);
        brokerThread.start();

        // Create a ConnectionFactory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getCurrentUrl());

        try {
            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(this);

            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue("TEST.FOO");

            // Create a MessageConsumer from the Session to the Topic or Queue
            consumer = session.createConsumer(destination);
        }  catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }

        isAlive=true;
        highestProposalAnswered=null;
    }

    @Override
    public void run() {
        try {
            // Wait for a message
            Message message=null;
            do {
                message = consumer.receive(1000);

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    String mess=getResponse(text);
                 }
            }while (isAlive);
            close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public String getResponse(String text) {
        if(text.startsWith("P")) {
//            If an acceptor receives a prepare request with number n greater than
//            that of any prepare request to which it has already responded,
            text=text.substring(1);
            int number= Integer.parseInt(text.split(",")[0]);
            String value= text.split(",")[2];

            if(highestProposalAnswered==null)
                return "AP";
            else if(highestProposalAnswered.nr< number )  {
               // then it responds to the request with a promise not to accept any more proposals numbered less
               // than n and with the highest-numbered proposal (if any) that it has accepted.
                String result="AP"+highestProposalAnswered.value.toString();  //wrong
                this.highestProposalAnswered=new Proposal(null, number,value);
                return result;
            }
            return "RP";
        }
        return null;
    }

    private void close() {
        try {
            consumer.close();
            session.close();
            connection.close();

        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void onException(JMSException e) {
        System.out.println("JMS Exception occurred.  Shutting down client.");
    }

    public void stopThread() {
        isAlive = false;
    }

    public String getCurrentUrl() {
        return broker.getCurrentUrl();
    }
}
