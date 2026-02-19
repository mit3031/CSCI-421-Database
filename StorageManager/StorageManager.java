package StorageManager;

import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import Common.Page;
import Catalog.Catalog;
import Catalog.TableSchema;
import java.io.File;
import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Common.Logger;


public class StorageManager {
    private static StorageManager storageManager;
    private String dbPath = "";
    private String databaseFilePath = "";


    public void CreateTable(TableSchema table) throws Exception {
        Catalog catalog = Catalog.getInstance();
        int firstFreePage;
        if (catalog.hasFreePages()) {
            firstFreePage = catalog.getFirstFreePage();
            catalog.removeFirstFreePage();
        } else {
            // Start at page 0 for first table, or use page size increments
            firstFreePage = catalog.getNumTables() * catalog.getPageSize();
        }
        table.setRootPageID(firstFreePage);
        // Add table to catalog BEFORE creating the page so writePage can find it
        catalog.addTable(table);
        BufferManager bufferManager = BufferManager.getInstance();
        //buffer manager creates the new page
        bufferManager.newPage(firstFreePage, table.getTableName());
    }

    public void DropTable(TableSchema table) throws Exception {
        Catalog catalog = Catalog.getInstance();
        BufferManager bufferManager = BufferManager.getInstance();
        bufferManager.dropTable(table.getTableName());
        catalog.dropTable(table.getTableName());
        // add this page to free page list
        catalog.addFirstFreePage(table.getRootPageID());
    }

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

        // Initialize BufferManager
        BufferManager.init(bufferSize, dbPath + File.separator + "database.bin");
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
     * Returns the first page associated with a table based on the provided table name.
     * This method assumes that tableName is a valid table
     * @param tableName The name of the table to select from
     * @return A list of pages associated with the table
     * @throws Exception Throws an exception if the table does not exist, or if there is an issue reading the page
     */
    public Page selectFirstPage(String tableName) throws Exception {
        BufferManager bufferManager = BufferManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        Page page = bufferManager.select(catalog.getAddressOfPage(tableName), tableName);
        return page;
    }


    /**
     * Returns a page in a table with an address and a table name.
     * @param address Address of the table
     * @param tableName Name of the table
     * @return the page at the address
     * @throws IOException If there is an issue reading the page
     */
    public Page select(int address, String tableName) throws IOException {
        BufferManager bufferManager = BufferManager.getInstance();
        return bufferManager.select(address, tableName);
    }

    public void insert(String tableName, List<List<Object>> rows) throws Exception {
        BufferManager bufferManager = BufferManager.getInstance();
        bufferManager.insert(tableName, rows);
    }

    public static void main(String[] args){
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        try {
            StorageManager.initDatabase(args[0], 400, 0);
            StorageManager store = StorageManager.getStorageManager();
            Catalog cat = Catalog.getInstance();

            Logger.log("Loading the attributes");
            List<Attribute> attributes = new ArrayList<>();
            attributes.add(
                    new Attribute("x",
                            new IntegerDefinition(null, true, false),
                            "5"
                    )
            );

            Logger.log("Creating Table Schema");
            TableSchema testTable = new TableSchema(
                    "TestTable",
                    attributes
            );
            store.CreateTable(testTable);

            Logger.log("Inserting elements into table");

            List<List<Object>> rows = new ArrayList<>();

            // Add 1, 2, and 3 as separate rows
            rows.add(Arrays.asList(1));
            rows.add(Arrays.asList(2));
            rows.add(Arrays.asList(3));

            store.insert("TestTable", rows);

            Logger.log("Attempting to extract that data");
            try {
                Page currentPage = store.selectFirstPage("TestTable");
                Logger.log("Found Rows" + currentPage.getNumRows());
                for (int i = 0; i < currentPage.getNumRows(); i++){
                    System.out.println(currentPage.getRecord(i).toString());
                }
                while (currentPage.getNextPage() != -1){
                    for (int i = 0; i < currentPage.getNumRows(); i++){
                        System.out.println(currentPage.getRecord(i).toString());
                    }
                    currentPage = store.select(currentPage.getNextPage(), "TestTable");
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new SQLSyntaxErrorException("Error getting pages: " + e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}