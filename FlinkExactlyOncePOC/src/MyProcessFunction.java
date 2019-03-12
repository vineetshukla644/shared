import java.util.Date;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyProcessFunction extends ProcessFunction<Balance,Alert> {

	@Override
	public void processElement(Balance arg0, ProcessFunction<Balance, Alert>.Context arg1, Collector<Alert> arg2)
			throws Exception {
		
		String time =new Date().toString();
		arg2.collect(new Alert(arg0.accountName,arg0.balance,time));
		
	}

}
