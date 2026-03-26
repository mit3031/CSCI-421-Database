import StorageManager.StorageManager;
import Common.Logger;
import DMLParser.ParserDML;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import AttributeInfo.VarCharDefinition;
import AttributeInfo.AttributeTypeEnum;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test file for testing the SELECT parsing functionality
 * Creates tables using StorageManager, inserts data directly, and tests various SELECT queries
 * 
 * Test Coverage:
 * - Basic SELECT * queries
 * - Projection with unqualified column names
 * - Projection with qualified column names (table.column)
 * - Mixed qualified and unqualified columns
 * - Multiple tables with unique columns
 * - Cartesian product simulation with dot notation in attribute names
 * - Ambiguity detection for unqualified names in cartesian products
 * - Error handling for:
 *   - Non-existent columns
 *   - Non-existent tables
 *   - Invalid qualified names
 *   - Wrong table qualifiers
 *   - Qualified name with attribute from wrong table (e.g., courses.name when name is in students)
 *   - Ambiguous unqualified names (e.g., SELECT x when x exists as both a.x and b.x)
 *   - Wrong qualifiers in cartesian products (e.g., b.y when y is from table a)
 *   - Multiple columns with invalid entries
 * 
 * Tables created:
 * - students(id INTEGER PRIMARY KEY, name VARCHAR(50)) - 4 rows
 * - courses(id INTEGER PRIMARY KEY, title VARCHAR(100)) - 2 rows
 * - axb(a.x INTEGER, a.y INTEGER, b.x INTEGER, b.q VARCHAR(50)) - 3 rows
 *   (Simulates cartesian product of A(x,y) × B(x,q) to test ambiguity handling)
 * 
 * Note: The 'axb' table demonstrates how the FROM clause will work once cartesian 
 * products are implemented. Attributes have dot notation (a.x, b.x) to simulate 
 * the result of joining multiple tables with overlapping column names.
 */
public class SelectParseTest {
    
    public static void main(String[] args) {
        // Enable debug logging
        args = new String[] {"--debug"}; 
        Logger.initDebug(args);

        String dbPath = "SelectTestDB";

        try {
            // Initialize the database
            System.out.println("=== Initializing Database ===");
            StorageManager.initDatabase(dbPath, 4096, 20);
            StorageManager store = StorageManager.getStorageManager();
            Catalog catalog = Catalog.getInstance();
            System.out.println("Database initialized successfully!\n");

            // Create a simple table with 2 attributes using StorageManager
            System.out.println("=== Creating Test Table ===");
            
            // Create attributes
            List<Attribute> attributes = new ArrayList<>();
            attributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            attributes.add(new Attribute("name", new VarCharDefinition(false, false, 50)));
            
            // Create table schema
            TableSchema studentsTable = new TableSchema("students", attributes);
            
            // Create the table using StorageManager
            store.CreateTable(studentsTable);
            System.out.println("Table 'students' created successfully!\n");

            // Insert some test data using StorageManager
            System.out.println("=== Inserting Test Data ===");
            
            // Get the root page address for the table
            int pageAddress = catalog.getAddressOfPage("students");
            
            // Insert row 1: (1, "Alice")
            List<Object> row1 = new ArrayList<>();
            row1.add(5);
            row1.add("Alice");
            pageAddress = store.insertSingleRow("students", row1, pageAddress);
            System.out.println("Inserted: (1, Alice)");
            
            // Insert row 2: (2, "Bob")
            List<Object> row2 = new ArrayList<>();
            row2.add(2);
            row2.add("Bob");
            pageAddress = store.insertSingleRow("students", row2, pageAddress);
            System.out.println("Inserted: (2, Bob)");
            
            // Insert row 3: (3, "Charlie")
            List<Object> row3 = new ArrayList<>();
            row3.add(3);
            row3.add("Charlie");
            pageAddress = store.insertSingleRow("students", row3, pageAddress);
            System.out.println("Inserted: (3, Charlie)");
            
            // Insert row 4: (4, "Diana")
            List<Object> row4 = new ArrayList<>();
            row4.add(4);
            row4.add("Diana");
            pageAddress = store.insertSingleRow("students", row4, pageAddress);
            System.out.println("Inserted: (4, Diana)");
            
            System.out.println("Test data inserted successfully!\n");

            // Create a second table to test ambiguity scenarios
            System.out.println("=== Creating Second Test Table (Courses) ===");
            
            // Create attributes for courses table
            List<Attribute> courseAttributes = new ArrayList<>();
            courseAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            courseAttributes.add(new Attribute("title", new VarCharDefinition(false, false, 100)));
            
            // Create table schema
            TableSchema coursesTable = new TableSchema("courses", courseAttributes);
            
            // Create the table using StorageManager
            store.CreateTable(coursesTable);
            System.out.println("Table 'courses' created successfully!");
            
            // Insert course data
            int coursePageAddress = catalog.getAddressOfPage("courses");
            
            List<Object> course1 = new ArrayList<>();
            course1.add(101);
            course1.add("Database Systems");
            coursePageAddress = store.insertSingleRow("courses", course1, coursePageAddress);
            System.out.println("Inserted: (101, Database Systems)");
            
            List<Object> course2 = new ArrayList<>();
            course2.add(102);
            course2.add("Algorithms");
            coursePageAddress = store.insertSingleRow("courses", course2, coursePageAddress);
            System.out.println("Inserted: (102, Algorithms)");
            
            System.out.println("Course data inserted successfully!\n");

            // Create a third table to simulate cartesian product with dot notation
            System.out.println("=== Creating Third Test Table (AxB - simulates cartesian product) ===");
            
            // This table simulates what would happen if we did: SELECT * FROM A, B
            // where A has (x, y) and B has (x, q)
            // Result would have qualified attributes: a.x, a.y, b.x, b.q
            List<Attribute> axbAttributes = new ArrayList<>();
            axbAttributes.add(new Attribute("a.x", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            axbAttributes.add(new Attribute("a.y", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false)));
            axbAttributes.add(new Attribute("b.x", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false)));
            axbAttributes.add(new Attribute("b.q", new VarCharDefinition(false, false, 50)));
            
            // Create table schema with primary key on first attribute
            TableSchema axbTable = new TableSchema("axb", axbAttributes);
            
            // Create the table using StorageManager
            store.CreateTable(axbTable);
            System.out.println("Table 'axb' created successfully!");
            System.out.println("Attributes: a.x (int), a.y (int), b.x (int), b.q (varchar)");
            
            // Insert cartesian product data
            int axbPageAddress = catalog.getAddressOfPage("axb");
            
            // Row 1: a.x=1, a.y=10, b.x=100, b.q="alpha"
            List<Object> axbRow1 = new ArrayList<>();
            axbRow1.add(1);
            axbRow1.add(10);
            axbRow1.add(100);
            axbRow1.add("alpha");
            axbPageAddress = store.insertSingleRow("axb", axbRow1, axbPageAddress);
            System.out.println("Inserted: (1, 10, 100, alpha)");
            
            // Row 2: a.x=1, a.y=10, b.x=200, b.q="beta"
            List<Object> axbRow2 = new ArrayList<>();
            axbRow2.add(1);
            axbRow2.add(10);
            axbRow2.add(200);
            axbRow2.add("beta");
            axbPageAddress = store.insertSingleRow("axb", axbRow2, axbPageAddress);
            System.out.println("Inserted: (1, 10, 200, beta)");
            
            // Row 3: a.x=2, a.y=20, b.x=100, b.q="gamma"
            List<Object> axbRow3 = new ArrayList<>();
            axbRow3.add(2);
            axbRow3.add(20);
            axbRow3.add(100);
            axbRow3.add("gamma");
            axbPageAddress = store.insertSingleRow("axb", axbRow3, axbPageAddress);
            System.out.println("Inserted: (2, 20, 100, gamma)");
            
            System.out.println("AxB cartesian product data inserted successfully!\n");

            // Test 1: SELECT * (select all columns)
            System.out.println("=== Test 1: SELECT * FROM students ===");
            ParserDML.runCommand("SELECT * FROM students;");
            System.out.println();

            // Test 2: SELECT specific column (projection)
            System.out.println("=== Test 2: SELECT name FROM students ===");
            ParserDML.runCommand("SELECT name FROM students;");
            System.out.println();

            // Test 3: SELECT id (projection on primary key)
            System.out.println("=== Test 3: SELECT id FROM students ===");
            ParserDML.runCommand("SELECT id FROM students;");
            System.out.println();

            // Test 4: SELECT multiple columns
            System.out.println("=== Test 4: SELECT id, name FROM students ===");
            ParserDML.runCommand("SELECT id, name FROM students;");
            System.out.println();

            // Test 5: SELECT with qualified name (even though unambiguous)
            System.out.println("=== Test 5: SELECT students.id FROM students ===");
            ParserDML.runCommand("SELECT students.id FROM students;");
            System.out.println();

            // Test 6: SELECT multiple columns with qualified names
            System.out.println("=== Test 6: SELECT students.id, students.name FROM students ===");
            ParserDML.runCommand("SELECT students.id, students.name FROM students;");
            System.out.println();

            // Test 7: SELECT mix of qualified and unqualified
            System.out.println("=== Test 7: SELECT students.id, name FROM students ===");
            ParserDML.runCommand("SELECT students.id, name FROM students;");
            System.out.println();

            // Test 8: SELECT from courses table
            System.out.println("=== Test 8: SELECT * FROM courses ===");
            ParserDML.runCommand("SELECT * FROM courses;");
            System.out.println();

            // Test 9: SELECT with qualified name from courses
            System.out.println("=== Test 9: SELECT courses.title FROM courses ===");
            ParserDML.runCommand("SELECT courses.title FROM courses;");
            System.out.println();

            // Test 10: SELECT unique column (only in students, not in courses)
            System.out.println("=== Test 10: SELECT name FROM students (unique column) ===");
            ParserDML.runCommand("SELECT name FROM students;");
            System.out.println();

            // Test 11: SELECT unique column (only in courses, not in students)
            System.out.println("=== Test 11: SELECT title FROM courses (unique column) ===");
            ParserDML.runCommand("SELECT title FROM courses;");
            System.out.println();

            // Test 12: Multiple qualified columns
            System.out.println("=== Test 12: SELECT students.id, students.name FROM students ===");
            ParserDML.runCommand("SELECT students.id, students.name FROM students;");
            System.out.println();

            System.out.println("=== Testing Cartesian Product Simulation (AxB table) ===\n");

            // Test 13: SELECT * from cartesian product table
            System.out.println("=== Test 13: SELECT * FROM axb ===");
            System.out.println("(Should show all qualified attributes: a.x, a.y, b.x, b.q)");
            ParserDML.runCommand("SELECT * FROM axb;");
            System.out.println();

            // Test 14: SELECT unambiguous unqualified names
            System.out.println("=== Test 14: SELECT y, q FROM axb ===");
            System.out.println("(y and q are unambiguous - only appear in one table each)");
            ParserDML.runCommand("SELECT y, q FROM axb;");
            System.out.println();

            // Test 15: SELECT qualified names for ambiguous attributes
            System.out.println("=== Test 15: SELECT a.x, b.x FROM axb ===");
            System.out.println("(x is ambiguous, so must be qualified)");
            ParserDML.runCommand("SELECT a.x, b.x FROM axb;");
            System.out.println();

            // Test 16: SELECT mix of qualified and unqualified in cartesian product
            System.out.println("=== Test 16: SELECT a.x, y, b.x, q FROM axb ===");
            System.out.println("(Mix of qualified for ambiguous and unqualified for unambiguous)");
            ParserDML.runCommand("SELECT a.x, y, b.x, q FROM axb;");
            System.out.println();

            // Test 17: SELECT only unqualified unique attributes
            System.out.println("=== Test 17: SELECT y FROM axb ===");
            System.out.println("(Single unambiguous attribute)");
            ParserDML.runCommand("SELECT y FROM axb;");
            System.out.println();

            System.out.println("=== Starting Error Test Cases ===\n");

            // NOTE: Ambiguity test (SELECT id FROM students, courses) cannot be tested yet
            // because cartesian product is not implemented in fromParse().
            // Once FROM clause supports multiple tables, that test should be added.
            // Expected behavior: Should throw "Ambiguous column name: id" error

            // Error Test 1: Column doesn't exist
            System.out.println("=== Error Test 1: SELECT nonexistent FROM students (should fail) ===");
            try {
                ParserDML.runCommand("SELECT nonexistent FROM students;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 2: Table doesn't exist
            System.out.println("=== Error Test 2: SELECT * FROM nonexistenttable (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM nonexistenttable;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 3: Qualified name with wrong table
            System.out.println("=== Error Test 3: SELECT courses.name FROM students (should fail) ===");
            try {
                ParserDML.runCommand("SELECT courses.name FROM students;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 4: Invalid qualified name format
            System.out.println("=== Error Test 4: SELECT table.attr.extra FROM students (should fail) ===");
            try {
                ParserDML.runCommand("SELECT table.attr.extra FROM students;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 5: Multiple columns with one invalid
            System.out.println("=== Error Test 5: SELECT id, badcolumn FROM students (should fail) ===");
            try {
                ParserDML.runCommand("SELECT id, badcolumn FROM students;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 6: Qualified name with attribute from different table
            System.out.println("=== Error Test 6: SELECT students.title FROM students (should fail) ===");
            try {
                ParserDML.runCommand("SELECT students.title FROM students;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 7: Wrong qualification - attribute exists but in different table
            System.out.println("=== Error Test 7: SELECT courses.name FROM courses (should fail) ===");
            System.out.println("(name exists in students, not courses)");
            try {
                ParserDML.runCommand("SELECT courses.name FROM courses;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            System.out.println("=== Cartesian Product Error Tests ===\n");

            // Error Test 8: Ambiguous unqualified name in cartesian product
            System.out.println("=== Error Test 8: SELECT x FROM axb (should fail) ===");
            System.out.println("(x is ambiguous - exists as both a.x and b.x)");
            try {
                ParserDML.runCommand("SELECT x FROM axb;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 9: Wrong table qualifier in cartesian product
            System.out.println("=== Error Test 9: SELECT b.y FROM axb (should fail) ===");
            System.out.println("(y belongs to table a, not table b)");
            try {
                ParserDML.runCommand("SELECT b.y FROM axb;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 10: Wrong table qualifier for q
            System.out.println("=== Error Test 10: SELECT a.q FROM axb (should fail) ===");
            System.out.println("(q belongs to table b, not table a)");
            try {
                ParserDML.runCommand("SELECT a.q FROM axb;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // Error Test 11: Mix of valid and ambiguous columns
            System.out.println("=== Error Test 11: SELECT y, x FROM axb (should fail) ===");
            System.out.println("(y is valid but x is ambiguous)");
            try {
                ParserDML.runCommand("SELECT y, x FROM axb;");
                System.out.println("ERROR: Should have thrown an exception!");
            } catch (SQLSyntaxErrorException e) {
                System.out.println("✓ Correctly caught error: " + e.getMessage());
            }
            System.out.println();

            // NOTE: Once cartesian product is implemented, add this test:
            // Error Test: SELECT courses.name FROM students, courses (should fail)
            // This tests that even when both tables are in FROM, you cannot qualify
            // an attribute with the wrong table name (name is in students, not courses)

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
