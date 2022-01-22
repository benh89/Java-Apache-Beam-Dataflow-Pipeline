class Loman extends Noodle {

    Loman() {

        super(3.0, 1.5, "irregular", "eggs, flour, salt");
        this.texture = "integrated with special source and components";

    }

    // Add the new cook() method below:
    @Override
    public void cook() {

        System.out.println("New way to cook. Taste is really good!");
        System.out.println("Boiling is just a traditional method.");

        this.texture = "cooked";

    }

}
