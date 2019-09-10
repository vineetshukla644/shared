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
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
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
		
		   CheckpointConfig config = env.getCheckpointConfig();
		   
		   config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		
		
		   env.enableCheckpointing(1000);

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
	        properties.setProperty("group.id", "balance7");
	        
	        
	       
	        
	        
	        
	        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<>("balance7", new SimpleStringSchema(), properties)).setParallelism(2);
	       
	       
	
            
	        
	     
	        
	        DataStream<Balance> balance = stream
	            .flatMap(new FlatMapFunction<String, Balance>() {
	                @Override
	                public void flatMap(String value, Collector<Balance> out) {
	                	String time =new Date().toString();
	                	String a[]= value.split(":");
	                    out.collect(new Balance(a[0],Long.parseLong(a[1])));
	                    
	                    }
	                }
	            ).setParallelism(2);
	        
	     
	        
	       
	        
	        
	        DataStream<Balance> reducedStream =    balance.keyBy("accountName").reduce(new MyReduceFunction()).setParallelism(2);
	        
	        
	        
	      //  DataStream<Alert> alertStream = balance.process(new MyProcessFunction());
	        
	        

	        Properties properties2 = new Properties();
	        properties2.setProperty("bootstrap.servers", "localhost:9092");
	        
	    /*   FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
	                "localhost:9092",            // broker list
	                "testoutput",                  // target topic
	                new SimpleStringSchema());   // serialization schema
	       */
	       
	       FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
	        		"testoutput",new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),properties2,Semantic.EXACTLY_ONCE);

	        myProducer.setWriteTimestampToKafka(true);
	        
	       
	        
	     DataStream<String> stream1 =reducedStream.map(new MapFunction<Balance, String>() {
	        		    @Override
	        		    public String map(Balance a) throws Exception {
	        		        return a.toString();
	        		    }
	        		}).setParallelism(2); 
	        
	        
	       
	        
	        
	     stream1.addSink(myProducer).setParallelism(2);
	        
	     env.execute("Balance Aggregate");
		
		
		
	}
	

}
