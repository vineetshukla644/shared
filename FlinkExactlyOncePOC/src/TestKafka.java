import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestKafka {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 System.out.println("i am here");
		 
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 10000; i++)
			 
		 {   System.out.println(i + " start ");
         
		 			producer.send(new ProducerRecord<String, String>("balance", "avishi:10"));
		 
		 			Thread.sleep(1000L);
		 			
		 			producer.send(new ProducerRecord<String, String>("balance", "anvi:20"));
		 
		 			Thread.sleep(1000L);
		 }
		 
		
		 
		 /*
		 
		
		 producer.send(new ProducerRecord<String, String>("balance1", "avishi:10"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "avishi:20"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "avishi:30"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "avishi:40"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "avishi:50"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "anvi:10"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "anvi:10"));
	     
		 producer.send(new ProducerRecord<String, String>("balance1", "anvi:10"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "anvi:10"));
		 
		 producer.send(new ProducerRecord<String, String>("balance1", "anvi:50"));
		 
		
		*/
		 
		 
		 System.out.println("i am here now");
		 
		 producer.close();
		 
		 System.out.println("i am here now 1");

	}

}
