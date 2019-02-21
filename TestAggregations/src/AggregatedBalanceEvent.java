
public class AggregatedBalanceEvent {
	
	
	public String entityname;
	public long balance;
	
	public  AggregatedBalanceEvent()
	{
		
	}

	public  AggregatedBalanceEvent(String entityname, long balance)
	{
		this.entityname = entityname;
        this.balance = balance;
        
	}
	
	
	 @Override
     public String toString() {
         return  entityname + " : " + balance  ;
     }

	 
	 
}
