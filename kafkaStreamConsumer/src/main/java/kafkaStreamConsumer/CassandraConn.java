package kafkaStreamConsumer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConn {
	
    private Cluster cluster;
    private Session session;
 
    public void connect(String node, Integer port) {
        Cluster cluster = Cluster.builder().addContactPoint(node).build();
        session = cluster.connect();
    }
 
    public Session getSession() {
        return this.session;
    }
 
    public void close() {
        session.close();
        cluster.close();
    }

}
