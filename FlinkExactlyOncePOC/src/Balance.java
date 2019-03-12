import java.util.Date;

public  class Balance {

	        public String accountName;
	        public long balance;
	       

	        public Balance() {}

	        public Balance(String accountName, long balance) {
	            this.accountName = accountName;
	            this.balance = balance;
	        }
	        
	        @Override
	        public String toString() {
	            return accountName + " : " + balance + " : " +  new Date() ;
	        }

	       
	    }