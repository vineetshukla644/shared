
public class BalanceEvent {

	public String accountname;
	public String entityname;
	public long balance;
	
	
	 public BalanceEvent() {}

     public BalanceEvent(String accountname,String entityname,  long balance) {
         this.accountname = accountname;
         this.balance = balance;
         this.entityname=entityname;
     }
     
     @Override
     public String toString() {
         return accountname + " : " + entityname + " : " + balance  ;
     }

     
     
	
}
