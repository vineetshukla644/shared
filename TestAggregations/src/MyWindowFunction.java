import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MyWindowFunction implements WindowFunction<BalanceEvent, BalanceEvent, Tuple, TimeWindow>{

	

	

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<BalanceEvent> input, Collector<BalanceEvent> out)
			throws Exception {

        Iterator<BalanceEvent> t = input.iterator();
		
		BalanceEvent lastelement = null;
		
		while(t.hasNext())
			
		{
			lastelement = t.next();
		}
	
		out.collect(lastelement);
		
		
	}

}
