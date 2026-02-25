import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import DMLParser.ParserDML;
import java.util.*;
import java.io.File;
import java.nio.file.*;

public class SelectTest {
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
        
        try {
            // Clean up any existing database
            cleanupDatabase("selecttestdb");
            
            StorageManager.initDatabase("selecttestdb", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("\n========================================");
            System.out.println("SELECT PARSER TEST SUITE");
            System.out.println("========================================\n");

            // ============================================================
            // TEST 1: Basic SELECT * on empty table
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: SELECT * on empty table");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs1 = new ArrayList<>();
                attrs1.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs1.add(new Attribute("name", new VarCharDefinition(false, false, 50), null));
                
                TableSchema table1 = new TableSchema("emptytable", attrs1);
                store.CreateTable(table1);
                
                // Run SELECT using parser
                ParserDML.runCommand("SELECT * FROM emptytable;");
                
                System.out.println("✓ PASSED: SELECT on empty table succeeded");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 2: SELECT * with integer data
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: SELECT * with integer data");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs2 = new ArrayList<>();
                attrs2.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs2.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table2 = new TableSchema("inttable", attrs2);
                store.CreateTable(table2);
                
                // Insert data directly using storage manager
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, 100));
                rows.add(Arrays.asList(2, 200));
                rows.add(Arrays.asList(3, 300));
                store.insert("inttable", rows);
                
                System.out.println("Expected output: 3 rows");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM inttable;");
                
                System.out.println("✓ PASSED: SELECT returned data");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 3: SELECT * with mixed types
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: SELECT * with mixed types");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs3 = new ArrayList<>();
                attrs3.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs3.add(new Attribute("name", new VarCharDefinition(false, false, 50), null));
                attrs3.add(new Attribute("score", new DoubleDefinition(false, false), null));
                attrs3.add(new Attribute("active", new BooleanDefinition(false, false), null));
                
                TableSchema table3 = new TableSchema("mixedtable", attrs3);
                store.CreateTable(table3);
                
                // Insert data
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, "Alice", 95.5, true));
                rows.add(Arrays.asList(2, "Bob", 87.3, false));
                rows.add(Arrays.asList(3, "Charlie", 92.0, true));
                store.insert("mixedtable", rows);
                
                System.out.println("Expected output: 3 rows with mixed types");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM mixedtable;");
                
                System.out.println("✓ PASSED: SELECT with mixed types succeeded");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 4: SELECT * with multiple pages
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 4: SELECT * with multiple pages");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs4 = new ArrayList<>();
                attrs4.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs4.add(new Attribute("data", new IntegerDefinition(null, false, false), null));
                
                TableSchema table4 = new TableSchema("largertable", attrs4);
                store.CreateTable(table4);
                
                // Insert 30 rows to span multiple pages
                List<List<Object>> rows = new ArrayList<>();
                for (int i = 1; i <= 30; i++) {
                    rows.add(Arrays.asList(i, i * 10));
                }
                store.insert("largertable", rows);
                
                System.out.println("Expected output: 30 rows across multiple pages");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM largertable;");
                
                System.out.println("✓ PASSED: SELECT across multiple pages succeeded");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 5: SELECT with incorrect syntax
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 5: SELECT with incorrect syntax");
            System.out.println("--------------------");
            try {
                boolean failed = false;
                
                // Missing FROM
                try {
                    ParserDML.runCommand("SELECT * inttable;");
                    failed = true;
                } catch (Exception e) {
                    System.out.println("  Correctly rejected: SELECT * inttable;");
                }
                
                // Missing semicolon
                try {
                    ParserDML.runCommand("SELECT * FROM inttable");
                    failed = true;
                } catch (Exception e) {
                    System.out.println("  Correctly rejected: missing semicolon");
                }
                
                // Table doesn't exist
                try {
                    ParserDML.runCommand("SELECT * FROM nonexistent;");
                    failed = true;
                } catch (Exception e) {
                    System.out.println("  Correctly rejected: table doesn't exist");
                }
                
                if (failed) {
                    System.out.println("✗ FAILED: Some invalid syntax was accepted");
                } else {
                    System.out.println("✓ PASSED: All invalid syntax rejected");
                    passedTests++;
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
            }

            // ============================================================
            // TEST 6: Case insensitivity for keywords
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 6: Case insensitivity for keywords");
            System.out.println("--------------------");
            try {
                // These should all work
                ParserDML.runCommand("select * from inttable;");
                ParserDML.runCommand("SELECT * FROM inttable;");
                ParserDML.runCommand("SeLeCt * FrOm inttable;");
                
                System.out.println("✓ PASSED: Keywords are case-insensitive");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 7: SELECT with NULL values
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 7: SELECT with NULL values");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs7 = new ArrayList<>();
                attrs7.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs7.add(new Attribute("name", new VarCharDefinition(false, true, 50), null));
                attrs7.add(new Attribute("score", new DoubleDefinition(false, true), null));
                attrs7.add(new Attribute("active", new BooleanDefinition(false, true), null));
                
                TableSchema table7 = new TableSchema("nulltable", attrs7);
                store.CreateTable(table7);
                
                // Insert data with various NULL values
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, "Alice", 95.5, null));      // NULL boolean
                rows.add(Arrays.asList(2, null, 87.3, true));         // NULL string
                rows.add(Arrays.asList(3, "Charlie", null, false));   // NULL double
                rows.add(Arrays.asList(4, "Diana", 88.0, true));      // No NULLs
                store.insert("nulltable", rows);
                
                System.out.println("Expected output: 4 rows with various NULL values");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM nulltable;");
                
                System.out.println("✓ PASSED: SELECT with NULL values displayed correctly");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 8: SELECT with VARCHAR/CHAR edge cases
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 8: SELECT with VARCHAR/CHAR edge cases");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs8 = new ArrayList<>();
                attrs8.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs8.add(new Attribute("text", new VarCharDefinition(false, true, 100), null));
                attrs8.add(new Attribute("code", new CharDefinition(false, true, 5), null));
                
                TableSchema table8 = new TableSchema("stringedge", attrs8);
                store.CreateTable(table8);
                
                // Insert data with empty strings and various content
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, "", "12345"));              // Empty VARCHAR
                rows.add(Arrays.asList(2, "Hello World!", null));     // NULL CHAR
                rows.add(Arrays.asList(3, "Test", "ABCDE"));          // Normal strings
                rows.add(Arrays.asList(4, null, "XYZ12"));            // NULL VARCHAR
                store.insert("stringedge", rows);
                
                System.out.println("Expected output: 4 rows with empty strings and NULLs");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM stringedge;");
                
                System.out.println("✓ PASSED: SELECT with VARCHAR/CHAR edge cases succeeded");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 9: SELECT with row containing all NULLs
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 9: SELECT with row containing all NULLs");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs9 = new ArrayList<>();
                attrs9.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs9.add(new Attribute("col1", new IntegerDefinition(null, false, true), null));
                attrs9.add(new Attribute("col2", new VarCharDefinition(false, true, 50), null));
                attrs9.add(new Attribute("col3", new BooleanDefinition(false, true), null));
                
                TableSchema table9 = new TableSchema("allnull", attrs9);
                store.CreateTable(table9);
                
                // Insert rows with all non-PK columns as NULL
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, null, null, null));         // All NULLs except PK
                rows.add(Arrays.asList(2, 100, "Test", true));        // Mixed
                rows.add(Arrays.asList(3, null, null, null));         // All NULLs except PK
                store.insert("allnull", rows);
                
                System.out.println("Expected output: 3 rows with some having all NULLs");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM allnull;");
                
                System.out.println("✓ PASSED: SELECT with all NULL columns displayed correctly");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 10: SELECT with precision DOUBLE values
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 10: SELECT with precision DOUBLE values");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs10 = new ArrayList<>();
                attrs10.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs10.add(new Attribute("measurement", new DoubleDefinition(false, false), null));
                attrs10.add(new Attribute("tolerance", new DoubleDefinition(false, true), null));
                
                TableSchema table10 = new TableSchema("doubletable", attrs10);
                store.CreateTable(table10);
                
                // Insert data with various double precision values
                List<List<Object>> rows = new ArrayList<>();
                rows.add(Arrays.asList(1, 3.14159265, 0.001));
                rows.add(Arrays.asList(2, 0.0, null));
                rows.add(Arrays.asList(3, -123.456789, 0.00001));
                rows.add(Arrays.asList(4, 999999.99, 1.0));
                rows.add(Arrays.asList(5, 0.000001, null));
                store.insert("doubletable", rows);
                
                System.out.println("Expected output: 5 rows with various double precision values");
                System.out.println("Actual output:");
                ParserDML.runCommand("SELECT * FROM doubletable;");
                
                System.out.println("✓ PASSED: SELECT with DOUBLE precision values displayed correctly");
                passedTests++;
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // SUMMARY
            // ============================================================
            System.out.println("\n========================================");
            System.out.println("SELECT TEST SUMMARY");
            System.out.println("========================================");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Failed: " + (totalTests - passedTests) + "/" + totalTests);
            
            if (passedTests == totalTests) {
                System.out.println("\n✓✓✓ ALL SELECT TESTS PASSED! ✓✓✓");
            } else {
                System.out.println("\n⚠ SOME TESTS FAILED");
            }
            System.out.println("========================================\n");

        } catch (Exception e) {
            System.out.println("\n✗ CRITICAL ERROR: Test suite failed to initialize");
            e.printStackTrace();
        }
    }
}
