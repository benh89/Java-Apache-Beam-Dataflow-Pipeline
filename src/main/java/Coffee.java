public class Coffee {

    public Coffee(){

    }

    public void addSugar(int sugarCubes){
        System.out.println("Added " + sugarCubes + " sugar cubes!");
    }

    public static void main(String[] args){
        Coffee myCup = new Coffee();
        myCup.addSugar(7);
    }

}
