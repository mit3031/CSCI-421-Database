package StorageManager;

import Page;
import Catalog;
import BufferManager;

public class StorageManager {
    private static StorageManager storageManager;
    public static void createStorageManager() {

        storageManager = new StorageManager();
    }
    public CreateTable(TableSchema table){
        Catalog catalog = Catalog.getInstance();
        int firstFreePage = catalog.getFirstFreePage();
        catalog.setFirstFreePage(firstFreePage+catalog.getPageSize());
        BufferManager bufferManager = BufferManager.getInstance();
        //buffer manager creates the new page
        bufferManager.newPage(firstFreePage);
        catalog.addTable(table);
    }

    public DropTable(TableSchema table) {
        Catalog catalog = Catalog.getInstance();
        BufferManager bufferManager = BufferManger.getInstance();
        bufferManager.dropTable(table);
        //call to getaddressofPage func in catalog
        catalog.dropTable()
    }

    private StorageManager() {

    }

    // public static int getValue(...){
    //     return storageManager.method();
    // }


}
