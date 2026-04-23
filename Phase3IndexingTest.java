import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import Common.Page;
import DMLParser.ParserDML;
import java.util.*;
import java.io.File;

/**
 * Test for Phase 3 B+ Tree Indexing
 * All tests run with indexing ENABLED in a single database
 */
public class Phase3IndexingTest {
    
    /**
     * Recursively deletes a directory and all its contents
     */
    private static void cleanupDatabase(String dbPath) {
        File dbDir = new File(dbPath);
        if (dbDir.exists()) {
            try {
                deleteDirectory(dbDir);
                System.out.println("Cleaned up existing database: " + dbPath);
            } catch (Exception e) {
                System.err.println("Warning: Failed to cleanup database: " + e.getMessage());
            }
        }
    }
    
    /**
     * Helper method to recursively delete a directory
     */
    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
    
    public static void main(String[] args) {
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        
        int passedTests = 0;
        int totalTests = 0;
        
        System.out.println("\n========================================");
        System.out.println("PHASE 3 B+ TREE INDEXING TEST SUITE");
        System.out.println("========================================\n");

        try {
            // Initialize ONE database with indexing ENABLED
            cleanupDatabase("phase3test_db");
            StorageManager.initDatabase("phase3test_db", 400, 6, true);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("Database initialized with indexing ENABLED\n");

            // ============================================================
            // TEST 1: Basic Insert with B+ tree indexing
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: Basic Insert with B+ tree indexing");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs1 = new ArrayList<>();
                attrs1.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
                attrs1.add(new Attribute("value", new IntegerDefinition(null, false, false, false), null));
                
                TableSchema table1 = new TableSchema("indexed_table", attrs1);
                store.CreateTable(table1);
                
                // Verify B+ tree was created
                Catalog catalog = Catalog.getInstance();
                if (catalog.getIndexingEnabled() && table1.getIndex("id") != null) {
                    System.out.println("  ✓ B+ tree index created for primary key 'id'");
                }
                
                // Insert 10 rows
                long startTime = System.currentTimeMillis();
                for (int i = 1; i <= 10; i++) {
                    ParserDML.runCommand("INSERT indexed_table VALUES (" + i + " " + (i * 10) + ");");
                }
                long endTime = System.currentTimeMillis();
                
                // Display table contents via SELECT
                System.out.println("\n  Retrieving data with SELECT * FROM indexed_table:");
                ParserDML.runCommand("SELECT * FROM indexed_table;");
                
                // Verify data is in sorted order
                System.out.println("\n  Verifying insertion order...");
                Page page = store.selectFirstPage("indexed_table");
                int count = 0;
                boolean sorted = true;
                int lastId = -1;
                
                while (page != null) {
                    for (int i = 0; i < page.getNumRows(); i++) {
                        count++;
                        List<Object> row = page.getRecord(i);
                        int id = (int) row.get(0);
                        if (id <= lastId) {
                            sorted = false;
                        }
                        lastId = id;
                    }
                    if (page.getNextPage() == -1) break;
                    page = store.select(page.getNextPage(), "indexed_table");
                }
                
                if (count == 10 && sorted) {
                    System.out.println("  ✓ PASSED: Inserted 10 rows using B+ tree");
                    System.out.println("  ✓ Data is sorted by primary key");
                    System.out.println("  Time: " + (endTime - startTime) + " ms");
                    passedTests++;
                } else {
                    System.out.println("  ✗ FAILED: Expected 10 sorted rows, got " + count + ", sorted=" + sorted);
                }
            } catch (Exception e) {
                System.out.println("  ✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 2: UNIQUE constraint with B+ tree
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: UNIQUE constraint with B+ tree");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs2 = new ArrayList<>();
                attrs2.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
                attrs2.add(new Attribute("email", new VarCharDefinition(false, false, 50, true), null)); // UNIQUE
                
                TableSchema table2 = new TableSchema("unique_table", attrs2);
                store.CreateTable(table2);
                
                // Verify B+ tree was created for UNIQUE column
                if (table2.getIndex("email") != null) {
                    System.out.println("  ✓ B+ tree index created for UNIQUE attribute 'email'");
                }
                
                // Insert first row
                ParserDML.runCommand("INSERT unique_table VALUES (1 \"user1@test.com\");");
                System.out.println("  ✓ First insert successful");
                
                // Try to insert duplicate email - should fail
                boolean duplicateFailed = false;
                try {
                    ParserDML.runCommand("INSERT unique_table VALUES (2 \"user1@test.com\");");
                    // If we get here, the duplicate was NOT rejected (test fails)
                } catch (Exception e) {
                    // Any exception means the duplicate was rejected (test passes)
                    duplicateFailed = true;
                    System.out.println("  ✓ Duplicate rejected");
                }
                
                // Display table contents via SELECT
                System.out.println("\n  Retrieving data with SELECT * FROM unique_table:");
                ParserDML.runCommand("SELECT * FROM unique_table;");
                
                // Verify insertion order (records should be sorted by primary key)
                System.out.println("\n  Verifying insertion order...");
                Page page = store.selectFirstPage("unique_table");
                int count = 0;
                boolean sorted = true;
                int lastId = -1;
                
                while (page != null) {
                    for (int i = 0; i < page.getNumRows(); i++) {
                        count++;
                        List<Object> row = page.getRecord(i);
                        int id = (int) row.get(0);
                        if (id <= lastId) {
                            sorted = false;
                        }
                        lastId = id;
                    }
                    if (page.getNextPage() == -1) break;
                    page = store.select(page.getNextPage(), "unique_table");
                }
                
                if (duplicateFailed && sorted) {
                    System.out.println("  ✓ PASSED: UNIQUE constraint enforced via B+ tree");
                    System.out.println("  ✓ Data is sorted by primary key");
                    passedTests++;
                } else {
                    if (!duplicateFailed) {
                        System.out.println("  ✗ FAILED: UNIQUE constraint not enforced");
                    }
                    if (!sorted) {
                        System.out.println("  ✗ FAILED: Data not in sorted order");
                    }
                }
            } catch (Exception e) {
                System.out.println("  ✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 3: Primary key ordering with random insertion
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: Primary key ordering with B+ tree");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs3 = new ArrayList<>();
                attrs3.add(new Attribute("pk", new IntegerDefinition(null, true, false, false), null));
                attrs3.add(new Attribute("data", new VarCharDefinition(false, false, 20, false), null));
                
                TableSchema table3 = new TableSchema("ordered_table", attrs3);
                store.CreateTable(table3);
                
                // Insert in random order
                int[] insertOrder = {5, 2, 8, 1, 9, 3, 7, 4, 6, 10};
                System.out.println("  Inserting in random order: " + Arrays.toString(insertOrder));
                for (int pk : insertOrder) {
                    ParserDML.runCommand("INSERT ordered_table VALUES (" + pk + " \"data" + pk + "\");");
                }
                
                // Display table contents via SELECT
                System.out.println("\n  Retrieving data with SELECT * FROM ordered_table:");
                ParserDML.runCommand("SELECT * FROM ordered_table;");
                
                // Verify they're stored in sorted order
                System.out.println("\n  Verifying insertion order...");
                Page page = store.selectFirstPage("ordered_table");
                List<Integer> retrievedOrder = new ArrayList<>();
                
                while (page != null) {
                    for (int i = 0; i < page.getNumRows(); i++) {
                        List<Object> row = page.getRecord(i);
                        retrievedOrder.add((int) row.get(0));
                    }
                    if (page.getNextPage() == -1) break;
                    page = store.select(page.getNextPage(), "ordered_table");
                }
                
                // Check if sorted
                boolean isSorted = true;
                for (int i = 0; i < retrievedOrder.size(); i++) {
                    if (retrievedOrder.get(i) != i + 1) {
                        isSorted = false;
                        break;
                    }
                }
                
                if (isSorted && retrievedOrder.size() == 10) {
                    System.out.println("  ✓ PASSED: Data stored in PK order despite random insertion");
                    System.out.println("  Storage order: " + retrievedOrder);
                    passedTests++;
                } else {
                    System.out.println("  ✗ FAILED: Data not properly sorted");
                    System.out.println("  Expected: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
                    System.out.println("  Got: " + retrievedOrder);
                }
            } catch (Exception e) {
                System.out.println("  ✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // Cleanup
            System.out.println("\nShutting down database...");
            store.shutdown();

        } catch (Exception e) {
            System.out.println("\n✗ CRITICAL ERROR: Test suite failed");
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
            System.out.println("✓✓✓ ALL TESTS PASSED!");
        } else {
            System.out.println("✗✗✗ SOME TESTS FAILED");
        }
        System.out.println("========================================\n");
    }
}
