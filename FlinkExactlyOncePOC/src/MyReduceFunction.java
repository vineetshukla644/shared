import org.apache.flink.api.common.functions.ReduceFunction;

public class MyReduceFunction implements ReduceFunction<Balance> {

	@Override
	public Balance reduce(Balance value1, Balance value2) throws Exception {
		// TODO Auto-generated method stub
		
		Balance b= new Balance();
		
		b.accountName=value1.accountName;
		b.balance =value1.balance + value2.balance;
		return b;
	}

}
