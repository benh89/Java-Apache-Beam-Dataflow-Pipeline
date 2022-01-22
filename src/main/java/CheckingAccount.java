public class CheckingAccount{
    public String name;
    private int balance;
    private String id;

    public CheckingAccount(String inputName, int inputBalance, String inputId){
        name = inputName;
        balance = inputBalance;
        id = inputId;
    }

    public void addFunds(int fundsToAdd){
        balance += fundsToAdd;
    }

    public void getInfo(){
        System.out.println("This checking account belongs to " + name +". It has " + balance + " dollars in it.");
    }
    /*
    public static void main(String[] args){
        CheckingAccount myAccount = new CheckingAccount("Lisa", 2000, inputId: "2");
        System.out.println(myAccount.balance);
        myAccount.addFunds(5);
        System.out.println(myAccount.balance);
    }
     */
}