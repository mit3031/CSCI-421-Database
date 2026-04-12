import StorageManager.StorageManager;
import Common.Logger;
import DMLParser.ParserDML;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import AttributeInfo.VarCharDefinition;
import AttributeInfo.DoubleDefinition;
import AttributeInfo.AttributeTypeEnum;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test file for testing SELECT with ORDERBY functionality
 * Creates tables using StorageManager, inserts unsorted data, and tests various SELECT ORDERBY queries
 * 
 * Test Coverage:
 * - Basic SELECT * with ORDERBY
 * - SELECT with projection and ORDERBY
 * - ORDERBY with qualified column names (table.column)
 * - ORDERBY with unqualified column names
 * - ORDERBY on different data types (INTEGER, VARCHAR, DOUBLE)
 * - ORDERBY on cartesian product simulation with ambiguous columns
 * - Error handling for:
 *   - Non-existent columns in ORDERBY
 *   - Ambiguous columns in ORDERBY
 *   - Wrong table qualifiers in ORDERBY
 * 
 * Tables created:
 * - grades(student_id INTEGER, score INTEGER, grade VARCHAR(2)) - 5 rows (unsorted)
 * - products(id INTEGER, name VARCHAR(50), price DOUBLE) - 4 rows (unsorted)
 * - axb(a.x INTEGER, a.y INTEGER, b.x INTEGER, b.q VARCHAR(50)) - 4 rows
 *   (Simulates cartesian product to test ORDERBY with qualified names)
 */
public class SelectOrderByTest {
    
    public static void main(String[] args) {
        // Enable debug logging
        args = new String[] {"--debug"}; 
        Logger.initDebug(args);

        String dbPath = "SelectOrderByTestDB";

        try {
            // Initialize the database
            System.out.println("=== Initializing Database ===");
            StorageManager.initDatabase(dbPath, 4096, 20,false);
            StorageManager store = StorageManager.getStorageManager();
            Catalog catalog = Catalog.getInstance();
            System.out.println("Database initialized successfully!\n");

            // ========== Create grades table ==========
            System.out.println("=== Creating Grades Table ===");
            List<Attribute> gradesAttributes = new ArrayList<>();
            gradesAttributes.add(new Attribute("student_id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false,false)));
            gradesAttributes.add(new Attribute("score", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false,false)));
            gradesAttributes.add(new Attribute("grade", new VarCharDefinition(false, false, 2,false)));
            
            TableSchema gradesTable = new TableSchema("grades", gradesAttributes);
            store.CreateTable(gradesTable);
            System.out.println("Table 'grades' created successfully!");
            
            // Insert unsorted grade data
            int gradesPageAddress = catalog.getAddressOfPage("grades");
            
            List<Object> grade1 = new ArrayList<>();
            grade1.add(103);
            grade1.add(85);
            grade1.add("B");
            gradesPageAddress = store.insertSingleRow("grades", grade1, gradesPageAddress);
            
            List<Object> grade2 = new ArrayList<>();
            grade2.add(101);
            grade2.add(92);
            grade2.add("A");
            gradesPageAddress = store.insertSingleRow("grades", grade2, gradesPageAddress);
            
            List<Object> grade3 = new ArrayList<>();
            grade3.add(105);
            grade3.add(78);
            grade3.add("C");
            gradesPageAddress = store.insertSingleRow("grades", grade3, gradesPageAddress);
            
            List<Object> grade4 = new ArrayList<>();
            grade4.add(102);
            grade4.add(88);
            grade4.add("B");
            gradesPageAddress = store.insertSingleRow("grades", grade4, gradesPageAddress);
            
            List<Object> grade5 = new ArrayList<>();
            grade5.add(104);
            grade5.add(95);
            grade5.add("A");
            gradesPageAddress = store.insertSingleRow("grades", grade5, gradesPageAddress);
            
            System.out.println("Inserted 5 unsorted grade records\n");

            // ========== Create products table ==========
            System.out.println("=== Creating Products Table ===");
            List<Attribute> productsAttributes = new ArrayList<>();
            productsAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false,false)));
            productsAttributes.add(new Attribute("name", new VarCharDefinition(false, false, 50,false)));
            productsAttributes.add(new Attribute("price", new DoubleDefinition(false, false,false)));
            
            TableSchema productsTable = new TableSchema("products", productsAttributes);
            store.CreateTable(productsTable);
            System.out.println("Table 'products' created successfully!");
            
            // Insert unsorted product data
            int productsPageAddress = catalog.getAddressOfPage("products");
            
            List<Object> prod1 = new ArrayList<>();
            prod1.add(203);
            prod1.add("Keyboard");
            prod1.add(49.99);
            productsPageAddress = store.insertSingleRow("products", prod1, productsPageAddress);
            
            List<Object> prod2 = new ArrayList<>();
            prod2.add(201);
            prod2.add("Mouse");
            prod2.add(25.50);
            productsPageAddress = store.insertSingleRow("products", prod2, productsPageAddress);
            
            List<Object> prod3 = new ArrayList<>();
            prod3.add(204);
            prod3.add("Monitor");
            prod3.add(199.99);
            productsPageAddress = store.insertSingleRow("products", prod3, productsPageAddress);
            
            List<Object> prod4 = new ArrayList<>();
            prod4.add(202);
            prod4.add("Headset");
            prod4.add(79.99);
            productsPageAddress = store.insertSingleRow("products", prod4, productsPageAddress);
            
            System.out.println("Inserted 4 unsorted product records\n");

            // ========== Create AxB cartesian product simulation table ==========
            System.out.println("=== Creating AxB Table (Cartesian Product Simulation) ===");
            List<Attribute> axbAttributes = new ArrayList<>();
            axbAttributes.add(new Attribute("a.x", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false,false)));
            axbAttributes.add(new Attribute("a.y", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false,false)));
            axbAttributes.add(new Attribute("b.x", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false,false)));
            axbAttributes.add(new Attribute("b.q", new VarCharDefinition(false, false, 50,false)));
            
            TableSchema axbTable = new TableSchema("axb", axbAttributes);
            store.CreateTable(axbTable);
            System.out.println("Table 'axb' created successfully!");
            
            // Insert unsorted cartesian product data
            int axbPageAddress = catalog.getAddressOfPage("axb");
            
            List<Object> axb1 = new ArrayList<>();
            axb1.add(2);
            axb1.add(20);
            axb1.add(100);
            axb1.add("gamma");
            axbPageAddress = store.insertSingleRow("axb", axb1, axbPageAddress);
            
            List<Object> axb2 = new ArrayList<>();
            axb2.add(1);
            axb2.add(10);
            axb2.add(200);
            axb2.add("beta");
            axbPageAddress = store.insertSingleRow("axb", axb2, axbPageAddress);
            
            List<Object> axb3 = new ArrayList<>();
            axb3.add(3);
            axb3.add(30);
            axb3.add(50);
            axb3.add("alpha");
            axbPageAddress = store.insertSingleRow("axb", axb3, axbPageAddress);
            
            List<Object> axb4 = new ArrayList<>();
            axb4.add(1);
            axb4.add(10);
            axb4.add(100);
            axb4.add("delta");
            axbPageAddress = store.insertSingleRow("axb", axb4, axbPageAddress);
            
            System.out.println("Inserted 4 unsorted AxB records\n");

            System.out.println("=== Starting SELECT ORDERBY Tests ===\n");

            // Test 1: Basic SELECT * with ORDERBY
            System.out.println("=== Test 1: SELECT * FROM grades ORDERBY student_id ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY student_id;");
                System.out.println("✓ Test 1 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 1 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 2: SELECT * ordered by different column
            System.out.println("=== Test 2: SELECT * FROM grades ORDERBY score ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY score;");
                System.out.println("✓ Test 2 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 2 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 3: SELECT with projection and ORDERBY
            System.out.println("=== Test 3: SELECT student_id, grade FROM grades ORDERBY score ===");
            try {
                ParserDML.runCommand("SELECT student_id, grade FROM grades ORDERBY score;");
                System.out.println("✓ Test 3 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 3 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 4: ORDERBY with VARCHAR column
            System.out.println("=== Test 4: SELECT * FROM grades ORDERBY grade ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY grade;");
                System.out.println("✓ Test 4 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 4 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 5: ORDERBY with qualified name (even though unambiguous)
            System.out.println("=== Test 5: SELECT * FROM grades ORDERBY grades.score ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY grades.score;");
                System.out.println("✓ Test 5 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 5 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 6: Products table with DOUBLE type
            System.out.println("=== Test 6: SELECT * FROM products ORDERBY price ===");
            try {
                ParserDML.runCommand("SELECT * FROM products ORDERBY price;");
                System.out.println("✓ Test 6 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 6 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 7: ORDERBY with projection showing sorted column
            System.out.println("=== Test 7: SELECT name, price FROM products ORDERBY price ===");
            try {
                ParserDML.runCommand("SELECT name, price FROM products ORDERBY price;");
                System.out.println("✓ Test 7 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 7 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 8: ORDERBY on products.id
            System.out.println("=== Test 8: SELECT * FROM products ORDERBY id ===");
            try {
                ParserDML.runCommand("SELECT * FROM products ORDERBY id;");
                System.out.println("✓ Test 8 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 8 failed: " + e.getMessage());
            }
            System.out.println();

            System.out.println("=== Cartesian Product ORDERBY Tests ===\n");

            // Test 9: ORDERBY with unambiguous unqualified name in AxB
            System.out.println("=== Test 9: SELECT * FROM axb ORDERBY y ===");
            System.out.println("(y is unambiguous - only exists as a.y)");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY y;");
                System.out.println("✓ Test 9 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 9 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 10: ORDERBY with qualified name for ambiguous column
            System.out.println("=== Test 10: SELECT * FROM axb ORDERBY a.x ===");
            System.out.println("(x is ambiguous, must be qualified)");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY a.x;");
                System.out.println("✓ Test 10 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 10 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 11: ORDERBY b.x instead of a.x
            System.out.println("=== Test 11: SELECT * FROM axb ORDERBY b.x ===");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY b.x;");
                System.out.println("✓ Test 11 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 11 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 12: ORDERBY with VARCHAR in cartesian product
            System.out.println("=== Test 12: SELECT * FROM axb ORDERBY q ===");
            System.out.println("(q is unambiguous - only exists as b.q)");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY q;");
                System.out.println("✓ Test 12 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 12 failed: " + e.getMessage());
            }
            System.out.println();

            // Test 13: Projection with ORDERBY in cartesian product
            System.out.println("=== Test 13: SELECT a.x, y, q FROM axb ORDERBY y ===");
            try {
                ParserDML.runCommand("SELECT a.x, y, q FROM axb ORDERBY y;");
                System.out.println("✓ Test 13 passed");
            } catch (Exception e) {
                System.out.println("✗ Test 13 failed: " + e.getMessage());
            }
            System.out.println();

            System.out.println("=== Starting Error Test Cases ===\n");

            // Error Test 1: ORDERBY non-existent column
            System.out.println("=== Error Test 1: SELECT * FROM grades ORDERBY invalid (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY invalid;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message);
            }
            System.out.println();

            // Error Test 2: ORDERBY ambiguous column in cartesian product
            System.out.println("=== Error Test 2: SELECT * FROM axb ORDERBY x (should fail) ===");
            System.out.println("(x is ambiguous - exists as both a.x and b.x)");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY x;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message);
            }
            System.out.println();

            // Error Test 3: ORDERBY with wrong table qualifier
            System.out.println("=== Error Test 3: SELECT * FROM axb ORDERBY b.y (should fail) ===");
            System.out.println("(y belongs to table a, not table b)");
            try {
                ParserDML.runCommand("SELECT * FROM axb ORDERBY b.y;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message);
            }
            System.out.println();

            // Error Test 4: ORDERBY column from different table
            System.out.println("=== Error Test 4: SELECT * FROM grades ORDERBY price (should fail) ===");
            System.out.println("(price is in products table, not grades)");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY price;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message);
            }
            System.out.println();

            // Error Test 5: ORDERBY with invalid qualified name format
            System.out.println("=== Error Test 5: SELECT * FROM grades ORDERBY a.b.c (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM grades ORDERBY a.b.c;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message);
            }
            System.out.println();

            System.out.println("=== All Tests Completed Successfully! ===");

        } catch (SQLSyntaxErrorException e) {
            System.err.println("SQL Error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
