import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import Common.Page;
import DMLParser.ParserDML;

import java.util.*;
import java.io.File;

/** AI generated btw
 * Test file for testing UPDATE functionality
 * * Test Coverage:
 * - Basic UPDATE with literal values (no WHERE)
 * - UPDATE with WHERE clause filtering
 * - UPDATE with single-operation math expressions (e.g., salary = salary + 1000)
 * - UPDATE using attribute-to-attribute assignments
 * - Primary Key violation detection during UPDATE
 * - Data type validation during UPDATE
 * - NOT NULL constraint validation during UPDATE
 */
public class UpdateTest {

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
            // Clean up any existing database to ensure a fresh test run
            cleanupDatabase("updatetestdb");

            StorageManager.initDatabase("updatetestdb", 4096, 20,false);
            StorageManager store = StorageManager.getStorageManager();

            System.out.println("\n========================================");
            System.out.println("UPDATE PARSER TEST SUITE");
            System.out.println("========================================\n");

            // ============================================================
            // SETUP: Create Table and Insert Initial Data
            // ============================================================
            System.out.println("SETUP: Creating 'employees' table and inserting initial data...");
            List<Attribute> empAttrs = new ArrayList<>();
            empAttrs.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false,false))); // PK, Not Null
            empAttrs.add(new Attribute("name", new VarCharDefinition(false, false, 50,false)));                     // Not Null
            empAttrs.add(new Attribute("salary", new DoubleDefinition(false, false,false)));                        // Not Null
            empAttrs.add(new Attribute("bonus", new DoubleDefinition(false, true,false)));                          // Nullable

            TableSchema empTable = new TableSchema("employees", empAttrs);
            store.CreateTable(empTable);

            ParserDML.runCommand("INSERT employees VALUES (1 \"John\" 50000.0 0.0);");
            ParserDML.runCommand("INSERT employees VALUES (2 \"Jane\" 60000.0 500.0);");
            ParserDML.runCommand("INSERT employees VALUES (3 \"Bob\" 40000.0 0.0);");
            System.out.println("Initial data inserted successfully.\n");


            // ============================================================
            // TEST 1: Basic literal update (No WHERE clause)
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: Basic literal update (All rows)");
            System.out.println("--------------------");
            try {
                // Should update all rows' bonus to 1000.0
                ParserDML.runCommand("UPDATE employees SET bonus = 1000.0;");

                Page page = store.selectFirstPage("employees");
                boolean allUpdated = true;
                for (int i = 0; i < page.getNumRows(); i++) {
                    List<Object> row = page.getRecord(i);
                    if (!row.get(3).equals(1000.0)) {
                        allUpdated = false;
                    }
                }

                if (allUpdated && page.getNumRows() == 3) {
                    System.out.println("✓ PASSED: All rows updated with literal value");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Not all rows updated correctly");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }


            // ============================================================
            // TEST 2: Update with WHERE clause
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: Update with WHERE clause filter");
            System.out.println("--------------------");
            try {
                // Update only Jane's name
                ParserDML.runCommand("UPDATE employees SET name = \"Jane Doe\" WHERE id = 2;");

                Page page = store.selectFirstPage("employees");
                boolean janeUpdated = false;
                boolean othersUntouched = true;

                for (int i = 0; i < page.getNumRows(); i++) {
                    List<Object> row = page.getRecord(i);
                    int id = (int) row.get(0);
                    String name = (String) row.get(1);

                    if (id == 2 && name.equals("Jane Doe")) janeUpdated = true;
                    if (id == 1 && !name.equals("John")) othersUntouched = false;
                }

                if (janeUpdated && othersUntouched) {
                    System.out.println("✓ PASSED: Only target row updated based on WHERE clause");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: WHERE clause filter did not behave as expected");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }


            // ============================================================
            // TEST 3: Update with Math Expression
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: Update with Math Expression (salary = salary + 5000.0)");
            System.out.println("--------------------");
            try {
                // John's original salary is 50000.0, should become 55000.0
                ParserDML.runCommand("UPDATE employees SET salary = salary + 5000.0 WHERE id = 1;");

                Page page = store.selectFirstPage("employees");
                boolean mathSuccess = false;

                for (int i = 0; i < page.getNumRows(); i++) {
                    List<Object> row = page.getRecord(i);
                    if ((int) row.get(0) == 1 && row.get(2).equals(55000.0)) {
                        mathSuccess = true;
                    }
                }

                if (mathSuccess) {
                    System.out.println("✓ PASSED: Math expression evaluated correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Math expression did not yield correct result");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }


            // ============================================================
            // TEST 4: Attribute-to-Attribute Assignment
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 4: Attribute-to-Attribute Assignment (bonus = salary)");
            System.out.println("--------------------");
            try {
                // Bob's salary is 40000.0. We set his bonus to equal his salary.
                ParserDML.runCommand("UPDATE employees SET bonus = salary WHERE id = 3;");

                Page page = store.selectFirstPage("employees");
                boolean attrSuccess = false;

                for (int i = 0; i < page.getNumRows(); i++) {
                    List<Object> row = page.getRecord(i);
                    if ((int) row.get(0) == 3 && row.get(3).equals(40000.0)) {
                        attrSuccess = true;
                    }
                }

                if (attrSuccess) {
                    System.out.println("✓ PASSED: Attribute reference assigned correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Attribute reference failed");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }


            // ============================================================
            // TEST 5: Primary Key Violation Detection
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 5: Primary Key Violation Detection");
            System.out.println("--------------------");
            try {
                boolean pkViolationCaught = false;
                try {
                    // Try to change John's ID (1) to Jane's ID (2)
                    ParserDML.runCommand("UPDATE employees SET id = 2 WHERE id = 1;");
                } catch (Exception e) {
                    if (e.getMessage().contains("Primary Key") || e.getMessage().contains("duplicate")) {
                        pkViolationCaught = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }

                if (pkViolationCaught) {
                    System.out.println("✓ PASSED: Prevented duplicate Primary Key update");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Allowed duplicate Primary Key update");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: Unexpected error format: " + e.getMessage());
            }


            // ============================================================
            // TEST 6: Type Mismatch Validation
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 6: Type Mismatch Validation");
            System.out.println("--------------------");
            try {
                boolean typeErrorCaught = false;
                try {
                    // Try to put a string into a Double column
                    ParserDML.runCommand("UPDATE employees SET salary = \"High\" WHERE id = 1;");
                } catch (Exception e) {
                    if (e.getMessage().contains("convert") || e.getMessage().contains("type")) {
                        typeErrorCaught = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }

                if (typeErrorCaught) {
                    System.out.println("✓ PASSED: Caught type mismatch during update");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Allowed incorrect type in update");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: Unexpected error format: " + e.getMessage());
            }

            // ============================================================
            // TEST 7: NOT NULL Violation Detection
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 7: NOT NULL Violation Detection");
            System.out.println("--------------------");
            try {
                boolean nullErrorCaught = false;
                try {
                    // Try to set 'name' to NULL (defined as NOT NULL)
                    ParserDML.runCommand("UPDATE employees SET name = NULL WHERE id = 1;");
                } catch (Exception e) {
                    if (e.getMessage().contains("NULL") || e.getMessage().contains("null")) {
                        nullErrorCaught = true;
                        System.out.println("  Correctly detected: " + e.getMessage());
                    }
                }

                if (nullErrorCaught) {
                    System.out.println("✓ PASSED: Caught NOT NULL constraint violation");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Allowed NULL in NOT NULL column");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: Unexpected error format: " + e.getMessage());
            }

            // ============================================================
            // SUMMARY
            // ============================================================
            System.out.println("\n========================================");
            System.out.println("UPDATE TEST SUMMARY");
            System.out.println("========================================");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Failed: " + (totalTests - passedTests) + "/" + totalTests);

            if (passedTests == totalTests) {
                System.out.println("\n✓✓✓ ALL UPDATE TESTS PASSED! ✓✓✓");
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