package Catalog;

public class Catalog {
    private static Catalog catalog;
    public static void createCatalog() {
        catalog = new Catalog();
    }
    private Catalog() {

    }

    // public static int getValue(...){
    //     return catalog.method();
    // }

}
