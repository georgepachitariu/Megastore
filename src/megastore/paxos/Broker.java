package megastore.paxos;

import org.apache.activemq.broker.BrokerService;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Broker implements Runnable {
    private String port;

    public Broker(String port) {
        this.port=port;
    }

    @Override
    public void run() {
        BrokerService broker = new BrokerService();
        // configure the broker
        try {
            broker.addConnector(getCurrentUrl());
            broker.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getCurrentUrl() {
        try {
            return "tcp://"+ InetAddress.getLocalHost().getHostAddress()+ ":" +port;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

}
