package citi.citidata.kafkaex;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerDemo.class);

	public static void main(String[] args) {
		logger.info("Started testing.");
	       Properties properties = new Properties();

	        // kafka bootstrap server
	        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
	        properties.setProperty("key.serializer", StringSerializer.class.getName());
	        properties.setProperty("value.serializer", StringSerializer.class.getName());
	        // producer acks
	        properties.setProperty("acks", "1");
	        properties.setProperty("retries", "3");
	        properties.setProperty("linger.ms", "1");

	        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

	        // once
//            ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<String, String>("second_topic", Integer.toString(3), "message that has key: " + Integer.toString(3));
//            producer.send(producerRecord);
       
	        // loop
	        for (int key=0; key < 10; key++){
	            ProducerRecord<String, String> producerRecord =
	                    new ProducerRecord<String, String>("second_topic", Integer.toString(key), "message that has key: " + Integer.toString(key));
	            producer.send(producerRecord);
	        }

            producer.close();
    		logger.info("Finished testing.");
	}

}
