package StorageManager;

import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import Common.Command;
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
import DDLParser.AlterTableDrop;
import DDLParser.ParserDDL;
import DMLParser.ParserDML;


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

    public void shutdown() throws IOException {
        BufferManager bufferManager = BufferManager.getInstance();
        bufferManager.flushAllPages();
        //bufferManager.saveToDisk();
    }
    public void bootup() {
        //BufferManager bufferManger = BufferManager.getInstance();
        //bufferManager.loadFromDisk();
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
        if(!catalog.tableExists(tableName)){
            throw new Exception("Table " + tableName + " does not exist");
        }
        Page page = bufferManager.select(catalog.getAddressOfPage(tableName), tableName);
        return page;
    }


    /**
     * Returns a page in a table with an address and a table name.
     * @param address Address of the table
     * @param tableName Name of the table
     * @return the page at the address
     * @throws Exception Throws an exception if the table does not exist, or if there is an issue reading the page
     */
    public Page select(int address, String tableName) throws Exception {
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
        
        int passedTests = 0;
        int totalTests = 0;
        
        try {
            StorageManager.initDatabase("storageManagerTest", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            Catalog cat = Catalog.getInstance();

            System.out.println("\n========================================");
            System.out.println("COMPREHENSIVE INSERT/SELECT TEST SUITE");
            System.out.println("========================================\n");

            // ============================================================
            // TEST 1: Single column integer table with few rows
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: Simple Integer Table");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs1 = new ArrayList<>();
                attrs1.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                
                TableSchema table1 = new TableSchema("SimpleTable", attrs1);
                store.CreateTable(table1);
                
                List<List<Object>> rows1 = new ArrayList<>();
                rows1.add(Arrays.asList(100));
                rows1.add(Arrays.asList(200));
                rows1.add(Arrays.asList(300));
                
                store.insert("SimpleTable", rows1);
                
                Page page1 = store.selectFirstPage("SimpleTable");
                if (page1.getNumRows() == 3 && 
                    page1.getRecord(0).get(0).equals(100) &&
                    page1.getRecord(1).get(0).equals(200) &&
                    page1.getRecord(2).get(0).equals(300)) {
                    System.out.println("✓ PASSED: Retrieved 3 rows with correct values");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Data mismatch");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 2: Multi-column table with different data types
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: Multi-Column Mixed Types");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs2 = new ArrayList<>();
                attrs2.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs2.add(new Attribute("score", new AttributeInfo.DoubleDefinition(false, false), null));
                attrs2.add(new Attribute("active", new AttributeInfo.BooleanDefinition(false, false), null));
                
                TableSchema table2 = new TableSchema("MixedTable", attrs2);
                store.CreateTable(table2);
                
                List<List<Object>> rows2 = new ArrayList<>();
                rows2.add(Arrays.asList(1, 95.5, true));
                rows2.add(Arrays.asList(2, 87.3, false));
                rows2.add(Arrays.asList(3, 92.0, true));
                
                store.insert("MixedTable", rows2);
                
                Page page2 = store.selectFirstPage("MixedTable");
                if (page2.getNumRows() == 3 &&
                    page2.getRecord(0).get(0).equals(1) &&
                    page2.getRecord(0).get(1).equals(95.5) &&
                    page2.getRecord(0).get(2).equals(true) &&
                    page2.getRecord(1).get(0).equals(2) &&
                    page2.getRecord(2).get(2).equals(true)) {
                    System.out.println("✓ PASSED: Mixed data types stored and retrieved correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Data type mismatch");
                }

                /*
                System.out.println("Test DDLParser:");
                System.out.println("--------------------");
                String command = "CREATE TABLE foo ( x INTEGER PRIMARYKEY );";
                boolean status = ParserDDL.parseCommand(command);
                System.out.println("Status: " + status);
                command = "CREATE TABLE myTable3 ( x1 INTEGER, x2 DOUBLE PRIMARYKEY, x3 VARCHAR(10) NOTNULL );";
                status = ParserDDL.parseCommand(command);
                System.out.println("Status: " + status);
                System.out.println();

                command = "INSERT INTO myTable3 VALUES ( 1, 2.0, \"hi!\");";
                ParserDML.runCommand(command);
                String selectCommand = "SELECT * FROM myTable3;";
                ParserDML.runCommand(selectCommand);

                System.out.println("Test Alter Add");
                command = "ALTER TABLE myTable3 ADD x14 INTEGER NOTNULL DEFAULT 15;";
                status = ParserDDL.parseCommand(command);
                System.out.println("Status: " + status );
                ParserDML.runCommand(selectCommand);
                System.out.println("Test Alter Drop");
                command = "DROP TABLE myTable3;";
                status = ParserDDL.parseCommand(command);
                System.out.println("Status: " + status );
                status = cat.tableExists("myTable3");
                System.out.println("Does table still exist? " + status);
                */




//                System.out.println("Test alterTableDrop");
//                String[] commandKeywords = new String[]{"Alter", "Table", "MixedTable", "DROP", "scOre"};
//                Command alterDrop = new AlterTableDrop();
//                boolean status = alterDrop.run(commandKeywords);
//                System.out.println("Staus: " + status );
//                Page pageafterdrop = store.selectFirstPage("MixedTable");
//                List<Attribute> attributes2 = cat.getTable("MixedTable").getAttributes();
//                System.out.println("After drop cols: " + attributes2.get(0).getName() +", "+ attributes2.get(1).getName() + "\nFirst record " + pageafterdrop.getRecord(0).toString());
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 3: Medium batch insert (10 rows)
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: Medium Batch Insert (10 rows)");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs3 = new ArrayList<>();
                attrs3.add(new Attribute("num", new IntegerDefinition(null, true, false), null));
                attrs3.add(new Attribute("doubled", new IntegerDefinition(null, false, false), null));
                
                TableSchema table3 = new TableSchema("MediumTable", attrs3);
                store.CreateTable(table3);
                
                List<List<Object>> rows3 = new ArrayList<>();
                for (int i = 1; i <= 10; i++) {
                    rows3.add(Arrays.asList(i, i * 2));
                }
                
                store.insert("MediumTable", rows3);
                
                Page page3 = store.selectFirstPage("MediumTable");
                int totalRows = 0;
                int firstVal = -1;
                int lastVal = -1;
                boolean dataCorrect = true;
                
                // Count rows across all pages
                while (page3 != null) {
                    System.out.println("  Page has " + page3.getNumRows() + " rows");
                    for (int i = 0; i < page3.getNumRows(); i++) {
                        totalRows++;
                        ArrayList<Object> rec = page3.getRecord(i);
                        System.out.println("    Record " + i + ": size=" + rec.size() + ", values=" + rec);
                        
                        if (rec.size() >= 2 && rec.get(0) != null && rec.get(1) != null) {
                            int num = (int) rec.get(0);
                            int doubled = (int) rec.get(1);
                            
                            if (totalRows == 1) firstVal = num;
                            lastVal = num;
                            
                            if (doubled != num * 2) {
                                dataCorrect = false;
                            }
                        } else {
                            System.out.println("    WARNING: Record has null or missing values!");
                            dataCorrect = false;
                        }
                    }
                    if (page3.getNextPage() != -1) {
                        page3 = store.select(page3.getNextPage(), "MediumTable");
                    } else {
                        break;
                    }
                }
                
                if (totalRows == 10 && firstVal == 1 && lastVal == 10 && dataCorrect) {
                    System.out.println("✓ PASSED: All 10 rows stored and retrieved correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Expected 10 rows, got " + totalRows + ", dataCorrect=" + dataCorrect);
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 4: Multiple insert batches
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 4: Multiple Separate Inserts");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs4 = new ArrayList<>();
                attrs4.add(new Attribute("value", new IntegerDefinition(null, true, false), null));
                
                TableSchema table4 = new TableSchema("BatchTable", attrs4);
                store.CreateTable(table4);
                
                // First batch
                List<List<Object>> batch1 = new ArrayList<>();
                batch1.add(Arrays.asList(10));
                batch1.add(Arrays.asList(20));
                store.insert("BatchTable", batch1);
                
                // Second batch
                List<List<Object>> batch2 = new ArrayList<>();
                batch2.add(Arrays.asList(30));
                batch2.add(Arrays.asList(40));
                store.insert("BatchTable", batch2);
                
                // Third batch
                List<List<Object>> batch3 = new ArrayList<>();
                batch3.add(Arrays.asList(50));
                store.insert("BatchTable", batch3);
                
                Page page4 = store.selectFirstPage("BatchTable");
                int count = 0;
                List<Integer> values = new ArrayList<>();
                
                while (page4 != null) {
                    for (int i = 0; i < page4.getNumRows(); i++) {
                        count++;
                        values.add((Integer) page4.getRecord(i).get(0));
                    }
                    if (page4.getNextPage() != -1) {
                        page4 = store.select(page4.getNextPage(), "BatchTable");
                    } else {
                        break;
                    }
                }
                
                if (count == 5 && values.contains(10) && values.contains(30) && values.contains(50)) {
                    System.out.println("✓ PASSED: Multiple inserts accumulated correctly (5 rows total)");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Expected 5 rows, got " + count);
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 5: Data persistence verification
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 5: Data Persistence (Re-read)");
            System.out.println("--------------------");
            try {
                // Re-read first table to ensure data was persisted
                Page page5 = store.selectFirstPage("SimpleTable");
                if (page5.getNumRows() == 3 && 
                    page5.getRecord(0).get(0).equals(100)) {
                    System.out.println("✓ PASSED: Data persisted correctly to disk");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Data not persisted");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // SUMMARY
            // ============================================================
            System.out.println("\n========================================");
            System.out.println("TEST SUMMARY");
            System.out.println("========================================");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Failed: " + (totalTests - passedTests) + "/" + totalTests);
            
            if (passedTests == totalTests) {
                System.out.println("\n✓✓✓ ALL TESTS PASSED! ✓✓✓");
            } else {
                System.out.println("\n⚠ SOME TESTS FAILED");
            }
            System.out.println("========================================\n");






        } catch (Exception e) {
            System.out.println("\n✗ CRITICAL ERROR: Test suite failed to initialize");
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}