 public  class LimitsRule {

	        public String accountName;
	        public long minBalance;
	        

	        public LimitsRule() {}

	        public LimitsRule(String accountName, long minBalance) {
	            this.accountName = accountName;
	            this.minBalance = minBalance;
	            
	        }
	        
	        @Override
	        public String toString() {
	            return accountName + " : " + minBalance;
	        }
	       
	    }