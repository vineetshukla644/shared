import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;



public class TestClass {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		  
		    env.enableCheckpointing(20000);

	        // advanced options:

	        // set mode to exactly-once (this is the default)
	        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

	        // make sure 500 ms of progress happen between checkpoints
		   
	        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

	        // checkpoints have to complete within one minute, or are discarded
	        
	       env.getCheckpointConfig().setCheckpointTimeout(60000);

	        // allow only one checkpoint to be in progress at the same time
	       
	       env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		
	       
	       env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
					  10, // number of restart attempts
					  Time.of(30, TimeUnit.SECONDS) // delay
					));
	       
	       
		
		    Properties properties = new Properties();
	        properties.setProperty("bootstrap.servers", "localhost:9092");
	        properties.setProperty("group.id", "testoutput");
	        properties.setProperty("isolation.level", "read_committed");
	        
	        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<>("testoutput", new SimpleStringSchema(), properties));
	       
	       
	        DataStream<String> balance = stream
		            .flatMap(new FlatMapFunction<String, String>() {
		                @Override
		                public void flatMap(String value, Collector<String> out) {
		                	String time =new Date().toString();
		                	String a = value + " : " + time;
		                    out.collect(a);
		                    
		                    }
		                }
		            );
		        
		     
	        
	        
	        
	        balance.print();
	        
	        env.execute("Consume Alert Messages");
		
		
		
	}
	

}
