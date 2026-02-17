package StorageManager;

import Common.Page;
import Catalog.Catalog;
import Catalog.TableSchema;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import Common.Logger;


public class StorageManager {
    private static StorageManager storageManager;
    private String dbPath = "";
    private String databaseFilePath = "";


//    public void CreateTable(TableSchema table){
//        Catalog catalog = Catalog.getInstance();
//        int firstFreePage = catalog.getFirstFreePage();
//        catalog.removeFirstFreePage();
//        table.setRootPageID(firstFreePage);
//        BufferManager bufferManager = BufferManager.getInstance();
//        //buffer manager creates the new page
//        bufferManager.newPage(firstFreePage, table.getTableName());
//        catalog.addTable(table);
//    }
//
//    public void DropTable(TableSchema table) {
//        Catalog catalog = Catalog.getInstance();
//        BufferManager bufferManager = BufferManager.getInstance();
//        bufferManager.dropTable(table);
//        catalog.dropTable(table.getTableName());
//        // add this page to free page list
//        catalog.addFirstFreePage(table.getRootPageId());
//    }

    private StorageManager(String dbPath, int pageSize, int bufferSize) throws Exception {
        this.dbPath = dbPath;

        Logger.log("db path is " + dbPath);

        if (dbPath.trim().isEmpty()){
            throw new Exception("Not a valid directory");
        }

        // 1. Check if the data directory exists
        File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
            if (dbDir.mkdirs()) {
                Logger.log("Database directory created at: " + dbPath);
            } else {
                throw new Exception("Failed to create directory: " + dbPath);
            }
        }

        // 2. Create the database.bin file inside the directory
        // Use File.separator to ensure this works on both Windows and Linux/Mac
        File dbFile = new File(dbPath + File.separator + "database.bin");
        this.databaseFilePath = dbPath + File.separator + "database.bin";

        if (!dbFile.exists()) {
            try {
                if (dbFile.createNewFile()) {
                    Logger.log("Database file created: " + dbFile.getAbsolutePath());
                }
            } catch (Exception e) {
                throw new Exception("Could not create database.bin: " + e.getMessage());
            }
        } else {
            Logger.log("Database file already exists, skipping creation.");
        }

        // From here, init the catalog.
        Catalog.init(dbPath, pageSize);
    }

    // Updated to accept parameters needed for the constructor
    private static void createStorageManager(String dbPath, int pageSize, int bufferSize) throws Exception {
        storageManager = new StorageManager(dbPath, pageSize, bufferSize);
    }

    public static void initDatabase(String dbPath, int pageSize, int bufferSize) throws Exception {
        if (storageManager == null){
            createStorageManager(dbPath, pageSize, bufferSize);
        }
    }

    // Getter for the singleton instance
    public static StorageManager getStorageManager() {
        return storageManager;
    }

    /**
     * Currently operates as a select start returning all pages corresponding to a table
     * This method assumes that tableName is a valid table
     * @param tableName The name of the table to select from
     * @return A list of pages associated with the table
     * @throws Exception Throws an exception if the table does not exist, or if there is an issue reading the page
     */
    public List<Page> select(String tableName) throws Exception {
        BufferManager bufferManager = BufferManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        List<Page> pages = bufferManager.select(catalog.getAddressOfPage(tableName), tableName);
        return pages;
    }

    public void insert(String tableName, List<List<Objects>> rows){
        BufferManager bufferManager = BufferManager.getInstance();
    }

    public static void main(String[] args){
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        try {
            StorageManager.initDatabase(args[0], 400, 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}