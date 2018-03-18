package kafkaStreamConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

public class consumer {

	public static void main(String[] args) throws IOException {
		
		String topic = "TrumpAmerica";
		Integer count = 0;
		
		Properties props = new Properties();

		props.put("bootstrap.servers","localhost:9092");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id","twitGroup1");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("-------------------------------------");
        System.out.println("Listening to Topic: " + topic);
        System.out.println("-------------------------------------\n");
        
        Cluster cluster = null;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("tweet_space");
        
        createTable(session, topic);
        
       while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(600000);
            
            if(consumerRecords.count() > 0) {
            		for(ConsumerRecord<String, String> record: consumerRecords) {
            			//System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            			insertTweet(session, topic, record.value().toString().replace("\'", "\""));
            			count++;
            			if(count%10 ==0) {
            				System.out.println(" Count = " + count);
            			}
            		}
            		consumer.commitSync();
            }else{
            		break;
            	}
        }
        consumer.close();
        cluster.close();
    }
	
	public static void createKeyspace(Session session, String keyspaceName, String replicationStrategy, int replicationFactor) {
			  StringBuilder sb = 
			    new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
			      .append(keyspaceName).append(" WITH replication = {")
			      .append("'class':'").append(replicationStrategy)
			      .append("','replication_factor':").append(replicationFactor)
			      .append("};");
			         
			    String query = sb.toString();
			    session.execute(query);
	}
	
	public static void createTable(Session session, String table_name) {
	    StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
	      .append(table_name)
	      .append("(")
	      .append("id uuid PRIMARY KEY, ")
	      .append("tweet text);");
	 
	    String query = sb.toString();
	    session.execute(query);
	}
	
	public static void insertTweet(Session session, String table_name, String tweet) {
	    StringBuilder sb = new StringBuilder("INSERT INTO ")
	      .append(table_name)
	      .append("(id, tweet) ")
	      .append("VALUES (")
	      .append(UUIDs.timeBased())
	      .append(" , \'")
	      .append(tweet)
	      .append("\' );");
	 
	    String query = sb.toString();
	    session.execute(query);
	}
}