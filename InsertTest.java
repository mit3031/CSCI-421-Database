import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import Common.Page;
import DMLParser.ParserDML;
import java.util.*;
import java.io.File;
import java.nio.file.*;

public class InsertTest {
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
            cleanupDatabase("inserttestdb");
            
            StorageManager.initDatabase("inserttestdb", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("\n========================================");
            System.out.println("INSERT PARSER TEST SUITE");
            System.out.println("========================================\n");

            // ============================================================
            // TEST 1: Basic INSERT with integers
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: Basic INSERT with integers");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs1 = new ArrayList<>();
                attrs1.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs1.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table1 = new TableSchema("inttable", attrs1);
                store.CreateTable(table1);
                
                // Insert using parser
                ParserDML.runCommand("INSERT INTO inttable VALUES (1, 100);");
                
                // Verify
                Page page = store.selectFirstPage("inttable");
                if (page.getNumRows() == 1) {
                    List<Object> row = page.getRecord(0);
                    if ((int)row.get(0) == 1 && (int)row.get(1) == 100) {
                        System.out.println("✓ PASSED: Integer INSERT successful");
                        passedTests++;
                    } else {
                        System.out.println("✗ FAILED: Data mismatch");
                    }
                } else {
                    System.out.println("✗ FAILED: Expected 1 row, got " + page.getNumRows());
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 2: INSERT with strings (quoted)
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: INSERT with strings (quoted)");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs2 = new ArrayList<>();
                attrs2.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs2.add(new Attribute("name", new VarCharDefinition(false, false, 50), null));
                
                TableSchema table2 = new TableSchema("stringtable", attrs2);
                store.CreateTable(table2);
                
                // Insert with double quotes
                ParserDML.runCommand("INSERT INTO stringtable VALUES (1, \"Alice\");");
                // Insert with single quotes
                ParserDML.runCommand("INSERT INTO stringtable VALUES (2, 'Bob');");
                
                // Verify
                Page page = store.selectFirstPage("stringtable");
                if (page.getNumRows() == 2) {
                    List<Object> row1 = page.getRecord(0);
                    List<Object> row2 = page.getRecord(1);
                    if (row1.get(1).equals("Alice") && row2.get(1).equals("Bob")) {
                        System.out.println("✓ PASSED: String INSERT successful");
                        passedTests++;
                    } else {
                        System.out.println("✗ FAILED: String data mismatch");
                    }
                } else {
                    System.out.println("✗ FAILED: Expected 2 rows");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 3: INSERT with strings containing spaces
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: INSERT with strings containing spaces");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs3 = new ArrayList<>();
                attrs3.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs3.add(new Attribute("fullname", new VarCharDefinition(false, false, 100), null));
                
                TableSchema table3 = new TableSchema("spacetable", attrs3);
                store.CreateTable(table3);
                
                // Insert strings with spaces
                ParserDML.runCommand("INSERT INTO spacetable VALUES (1, \"John Doe\");");
                ParserDML.runCommand("INSERT INTO spacetable VALUES (2, \"Jane Mary Smith\");");
                
                // Verify
                Page page = store.selectFirstPage("spacetable");
                if (page.getNumRows() == 2) {
                    List<Object> row1 = page.getRecord(0);
                    List<Object> row2 = page.getRecord(1);
                    if (row1.get(1).equals("John Doe") && row2.get(1).equals("Jane Mary Smith")) {
                        System.out.println("✓ PASSED: Strings with spaces preserved");
                        passedTests++;
                    } else {
                        System.out.println("✗ FAILED: Spaces not preserved. Got: " + row1.get(1) + ", " + row2.get(1));
                    }
                } else {
                    System.out.println("✗ FAILED: Expected 2 rows");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 4: INSERT with mixed types
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 4: INSERT with mixed types");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs4 = new ArrayList<>();
                attrs4.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs4.add(new Attribute("name", new VarCharDefinition(false, false, 50), null));
                attrs4.add(new Attribute("score", new DoubleDefinition(false, false), null));
                attrs4.add(new Attribute("active", new BooleanDefinition(false, false), null));
                
                TableSchema table4 = new TableSchema("mixedtable", attrs4);
                store.CreateTable(table4);
                
                // Insert mixed types
                ParserDML.runCommand("INSERT INTO mixedtable VALUES (1, \"Alice\", 95.5, true);");
                ParserDML.runCommand("INSERT INTO mixedtable VALUES (2, \"Bob\", 87.3, false);");
                
                // Verify
                Page page = store.selectFirstPage("mixedtable");
                if (page.getNumRows() == 2) {
                    List<Object> row1 = page.getRecord(0);
                    if (row1.get(0).equals(1) && row1.get(1).equals("Alice") && 
                        row1.get(2).equals(95.5) && row1.get(3).equals(true)) {
                        System.out.println("✓ PASSED: Mixed type INSERT successful");
                        passedTests++;
                    } else {
                        System.out.println("✗ FAILED: Mixed type data mismatch");
                    }
                } else {
                    System.out.println("✗ FAILED: Expected 2 rows");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 5: INSERT multiple rows in one statement
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 5: INSERT multiple rows in one statement");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs5 = new ArrayList<>();
                attrs5.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs5.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table5 = new TableSchema("multitable", attrs5);
                store.CreateTable(table5);
                
                // Insert multiple rows
                ParserDML.runCommand("INSERT INTO multitable VALUES (1, 10), (2, 20), (3, 30);");
                
                // Verify
                Page page = store.selectFirstPage("multitable");
                if (page.getNumRows() == 3) {
                    System.out.println("✓ PASSED: Multiple row INSERT successful");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Expected 3 rows, got " + page.getNumRows());
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 6: Primary key violation detection
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 6: Primary key violation detection");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs6 = new ArrayList<>();
                attrs6.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs6.add(new Attribute("data", new IntegerDefinition(null, false, false), null));
                
                TableSchema table6 = new TableSchema("pktable", attrs6);
                store.CreateTable(table6);
                
                // First insert should succeed
                ParserDML.runCommand("INSERT INTO pktable VALUES (1, 100);");
                
                boolean violationDetected = false;
                try {
                    // Second insert with same PK should fail
                    ParserDML.runCommand("INSERT INTO pktable VALUES (1, 200);");
                } catch (Exception e) {
                    if (e.getMessage().contains("Primary key") || e.getMessage().contains("primary key")) {
                        violationDetected = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }
                
                if (violationDetected) {
                    System.out.println("✓ PASSED: Primary key violation detected");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Primary key violation not detected");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 7: Duplicate PK in same batch
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 7: Duplicate primary key in same batch");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs7 = new ArrayList<>();
                attrs7.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs7.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table7 = new TableSchema("pkbatch", attrs7);
                store.CreateTable(table7);
                
                boolean violationDetected = false;
                try {
                    // This should fail - duplicate PK in same batch
                    ParserDML.runCommand("INSERT INTO pkbatch VALUES (1, 10), (1, 20);");
                } catch (Exception e) {
                    if (e.getMessage().contains("Duplicate") || e.getMessage().contains("duplicate")) {
                        violationDetected = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }
                
                if (violationDetected) {
                    System.out.println("✓ PASSED: Duplicate PK in batch detected");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Duplicate PK in batch not detected");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 8: Type validation
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 8: Type validation");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs8 = new ArrayList<>();
                attrs8.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs8.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table8 = new TableSchema("typetable", attrs8);
                store.CreateTable(table8);
                
                boolean typeErrorDetected = false;
                try {
                    // This should fail - string where integer expected
                    ParserDML.runCommand("INSERT INTO typetable VALUES (1, \"notanumber\");");
                } catch (Exception e) {
                    if (e.getMessage().contains("Invalid") || e.getMessage().contains("type") || 
                        e.getMessage().contains("convert")) {
                        typeErrorDetected = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }
                
                if (typeErrorDetected) {
                    System.out.println("✓ PASSED: Type mismatch detected");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Type mismatch not detected");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 9: NULL handling
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 9: NULL handling");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs9 = new ArrayList<>();
                attrs9.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs9.add(new Attribute("value", new IntegerDefinition(null, false, true), null)); // nullable
                
                TableSchema table9 = new TableSchema("nulltable", attrs9);
                store.CreateTable(table9);
                
                // Insert with NULL - should succeed
                ParserDML.runCommand("INSERT INTO nulltable VALUES (1, NULL);");
                
                // Verify
                Page page = store.selectFirstPage("nulltable");
                if (page.getNumRows() == 1) {
                    List<Object> row = page.getRecord(0);
                    if (row.get(0).equals(1) && row.get(1) == null) {
                        System.out.println("✓ PASSED: NULL value handled correctly");
                        passedTests++;
                    } else {
                        System.out.println("✗ FAILED: NULL not stored correctly");
                    }
                } else {
                    System.out.println("✗ FAILED: Expected 1 row");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 10: Wrong number of columns
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 10: Wrong number of columns");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs10 = new ArrayList<>();
                attrs10.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs10.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
                
                TableSchema table10 = new TableSchema("coltable", attrs10);
                store.CreateTable(table10);
                
                boolean errorDetected = false;
                try {
                    // Too few columns
                    ParserDML.runCommand("INSERT INTO coltable VALUES (1);");
                } catch (Exception e) {
                    if (e.getMessage().contains("values") || e.getMessage().contains("expected")) {
                        errorDetected = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }
                
                if (errorDetected) {
                    System.out.println("✓ PASSED: Column count mismatch detected");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Column count mismatch not detected");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // SUMMARY
            // ============================================================
            System.out.println("\n========================================");
            System.out.println("INSERT TEST SUMMARY");
            System.out.println("========================================");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Failed: " + (totalTests - passedTests) + "/" + totalTests);
            
            if (passedTests == totalTests) {
                System.out.println("\n✓✓✓ ALL INSERT TESTS PASSED! ✓✓✓");
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
