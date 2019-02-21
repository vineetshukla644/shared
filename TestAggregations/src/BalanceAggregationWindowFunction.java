import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class BalanceAggregationWindowFunction  implements WindowFunction<BalanceEvent, AggregatedBalanceEvent, Tuple, TimeWindow> {

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<BalanceEvent> input, Collector<AggregatedBalanceEvent> out)
			throws Exception {
		
		    Iterator<BalanceEvent> t = input.iterator();
			
		  
			
		    long balance = 0;
		    String entity =null;
		    
		    
			while(t.hasNext())
				
			{
			   BalanceEvent be= t.next();
			   
			   balance = balance + be.balance;
			   
			   entity=be.entityname;
			}
		
		
		out.collect(new AggregatedBalanceEvent(entity,balance));
		
	}

}
