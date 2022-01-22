
public class ATM{
    public static int totalMoney = 1;

    public ATM(){
        totalMoney += 5;
    }

    public static void main(String[] args){

        System.out.println(ATM.totalMoney); //Line 1

        ATM myATM = new ATM();
        System.out.println(ATM.totalMoney); //Line 2
        System.out.println(myATM.totalMoney); //Line 3

        ATM mySecondATM = new ATM();
        System.out.println(ATM.totalMoney); //Line 4
        System.out.println(myATM.totalMoney); //Line 5
    }

}

