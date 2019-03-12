 public  class Alert {

	        public String accountName;
	        public long balance;
	        public String timestamp;

	        public Alert() {}

	        public Alert(String accountName, long balance, String timestamp) {
	            this.accountName = accountName;
	            this.balance = balance;
	            this.timestamp = timestamp;
	        }
	        
	        @Override
	        public String toString() {
	            return accountName + " : " + balance + " : " + timestamp ;
	        }
	       
	    }