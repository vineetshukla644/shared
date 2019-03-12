import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyBroadcastProcessFunction extends BroadcastProcessFunction<Balance,LimitsRule,Alert> {

	private final MapStateDescriptor<String, LimitsRule> ruleStateDescriptor = new MapStateDescriptor<>(
 			"RulesBroadcastState",
 			BasicTypeInfo.STRING_TYPE_INFO,
 			TypeInformation.of(new TypeHint<LimitsRule>() {}));

	@Override
	public void processBroadcastElement(LimitsRule arg0,BroadcastProcessFunction<Balance, LimitsRule, Alert>.Context arg1, Collector<Alert> arg2) throws Exception {
		// TODO Auto-generated method stub
		
		arg1.getBroadcastState(ruleStateDescriptor).put(arg0.accountName, arg0);
		
	}

	@Override
	public void processElement(Balance arg0, BroadcastProcessFunction<Balance, LimitsRule, Alert>.ReadOnlyContext arg1,
			Collector<Alert> arg2) throws Exception {
		// TODO Auto-generated method stub
		
		
		
		
		for(Map.Entry<String,LimitsRule> entry : arg1.getBroadcastState(ruleStateDescriptor).immutableEntries())
		{
		
			if(arg0.accountName.equals(entry.getKey()))
			{
				System.out.println("Inside upper if : " +  arg0.accountName);
				
				if(arg0.balance < entry.getValue().minBalance)
				{
					
					System.out.println("Inside lower if : " +  arg0.accountName + " : "+arg0.balance);
					String time =new Date().toString();
					
					arg2.collect(new Alert(arg0.accountName,arg0.balance,time));
				}
				
			}
			
			
	    }

}
	
	
}
