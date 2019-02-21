import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;



public class TestClass {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		    Properties properties = new Properties();
	        properties.setProperty("bootstrap.servers", "localhost:9092");
	        properties.setProperty("group.id", "balance");
	        
	        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<>("balance", new SimpleStringSchema(), properties));
	        
	        DataStream<BalanceEvent> balancevent = stream
		            .flatMap(new FlatMapFunction<String, BalanceEvent>() {
		                @Override
		                public void flatMap(String value, Collector<BalanceEvent> out) {
		                    String a[]= value.split(":");
		                    out.collect(new BalanceEvent(a[0],a[1],Long.parseLong(a[2])));
		                    
		                    }
		                }
		            );
	        
	        
	      
	      /*  DataStream<BalanceEvent> balanceevent1=  
	        		balancevent.keyBy("accountname")
	        		.timeWindow(Time.seconds(30)).apply(new MyWindowFunction());
	        	
	        DataStream<AggregatedBalanceEvent> balanceevent2= balanceevent1.keyBy("entityname").timeWindow(Time.seconds(30)).apply(new BalanceAggregationWindowFunction());
	      */
	       
	        DataStream<BalanceEvent> balanceevent1=  
	        		balancevent.keyBy("accountname")
	        		.timeWindow(Time.seconds(60)).process(new MyProcessFunction());
	        	
	        DataStream<AggregatedBalanceEvent> balanceevent2= balanceevent1.keyBy("entityname")
	        		.timeWindow(Time.seconds(30))
	        		.process(new AggregatedBalanceProcessWindowFunction());

	        
	        
	        balanceevent1.print();
	        
	        
	        balanceevent2.print();
	        
	        env.execute("Balance Alert");
		
		
		
	}
	

	
	
	
}
