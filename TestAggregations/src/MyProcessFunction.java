import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessFunction extends ProcessWindowFunction<BalanceEvent, BalanceEvent, Tuple, TimeWindow >  {

	@Override
	public void process(Tuple arg0, ProcessWindowFunction<BalanceEvent, BalanceEvent, Tuple, TimeWindow>.Context arg1,
			Iterable<BalanceEvent> arg2, Collector<BalanceEvent> arg3) throws Exception {
		
		
				Iterator<BalanceEvent> t = arg2.iterator();
						
				BalanceEvent lastelement = null;
						
						while(t.hasNext())
							
						{
							lastelement = t.next();
						}
					
						arg3.collect(lastelement);
						
						// TODO Auto-generated method stub
		
	}

	
	}

	
	

	


	

