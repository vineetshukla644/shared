import java.util.Iterator;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

    public class AggregatedBalanceProcessWindowFunction extends ProcessWindowFunction<BalanceEvent, AggregatedBalanceEvent, Tuple, TimeWindow >  {

	
	private  MapStateDescriptor<String, BalanceEvent> balanceStateDescriptor = new MapStateDescriptor<>(
 			"BalanceStateDescriptor",
 			BasicTypeInfo.STRING_TYPE_INFO,
 			TypeInformation.of(new TypeHint<BalanceEvent>() {}));
	
	private transient MapState<String, BalanceEvent> state;
	
	
	@Override
	public void process(Tuple arg0,
			ProcessWindowFunction<BalanceEvent, AggregatedBalanceEvent, Tuple, TimeWindow>.Context arg1,
			Iterable<BalanceEvent> arg2, Collector<AggregatedBalanceEvent> arg3) throws Exception {
		
		 state=arg1.globalState().getMapState(balanceStateDescriptor);
		
		 
		 Iterator<BalanceEvent> t = arg2.iterator();
			
		  
			
		    long balance = 0;
		    String entity =null;
		    
		    
			while(t.hasNext())
				
			{
			  
				
			   BalanceEvent be= t.next();
			   
			   balance = balance + be.balance;
			   
			   entity=be.entityname;
			   
			   state.put(be.accountname, be);
			   
			}
		
		
			arg3.collect(new AggregatedBalanceEvent(entity,balance));
		
		
	}

}
