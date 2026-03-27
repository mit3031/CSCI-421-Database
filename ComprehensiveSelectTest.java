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
 * Comprehensive test file for all SELECT functionality
 * 
 * Tests Coverage:
 * 1. Projection (SELECT *, SELECT a1, a2, ...)
 * 2. FROM clause (single table, cartesian product)
 * 3. WHERE clause (relational operators, logical operators, IS NULL)
 * 4. ORDERBY (single attribute, ascending)
 * 5. Qualified attribute names (table.attribute)
 * 6. Mathematical operations (+, -, *, /)
 * 7. Combinations of all features
 * 8. Order of operations: FROM → WHERE → ORDERBY → SELECT
 * 
 * Tables created:
 * - employees(id INTEGER PK, name VARCHAR(50), salary INTEGER, bonus INTEGER, department VARCHAR(20))
 * - departments(id INTEGER PK, name VARCHAR(20), budget DOUBLE)
 * - products(id INTEGER PK, name VARCHAR(50), price DOUBLE, quantity INTEGER, category VARCHAR(20))
 * - nullable_test(id INTEGER PK, value INTEGER, text VARCHAR(20))
 * 
 * Data includes:
 * - Various numeric values for testing relational operators
 * - NULL values for IS NULL testing
 * - Overlapping attribute names for qualified name testing
 * - Mix of data types (INTEGER, DOUBLE, VARCHAR)
 */
public class ComprehensiveSelectTest {
    
    public static void main(String[] args) {
        // Enable debug logging
        args = new String[] {"--debug"}; 
        Logger.initDebug(args);

        String dbPath = "ComprehensiveSelectTestDB";

        try {
            // Initialize the database
            System.out.println("=== Initializing Database ===");
            StorageManager.initDatabase(dbPath, 4096, 20);
            StorageManager store = StorageManager.getStorageManager();
            Catalog catalog = Catalog.getInstance();
            System.out.println("Database initialized successfully!\n");

            // ========== Create employees table ==========
            System.out.println("=== Creating Employees Table ===");
            List<Attribute> employeesAttributes = new ArrayList<>();
            employeesAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            employeesAttributes.add(new Attribute("name", new VarCharDefinition(false, false, 50)));
            employeesAttributes.add(new Attribute("salary", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false)));
            employeesAttributes.add(new Attribute("bonus", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false)));
            employeesAttributes.add(new Attribute("department", new VarCharDefinition(false, false, 20)));
            
            TableSchema employeesTable = new TableSchema("employees", employeesAttributes);
            store.CreateTable(employeesTable);
            System.out.println("Table 'employees' created successfully!");
            
            // Insert employee data
            int empPageAddress = catalog.getAddressOfPage("employees");
            
            List<Object> emp1 = new ArrayList<>();
            emp1.add(1);
            emp1.add("Alice");
            emp1.add(75000);
            emp1.add(5000);
            emp1.add("Engineering");
            empPageAddress = store.insertSingleRow("employees", emp1, empPageAddress);
            
            List<Object> emp2 = new ArrayList<>();
            emp2.add(2);
            emp2.add("Bob");
            emp2.add(65000);
            emp2.add(3000);
            emp2.add("Sales");
            empPageAddress = store.insertSingleRow("employees", emp2, empPageAddress);
            
            List<Object> emp3 = new ArrayList<>();
            emp3.add(3);
            emp3.add("Charlie");
            emp3.add(80000);
            emp3.add(7000);
            emp3.add("Engineering");
            empPageAddress = store.insertSingleRow("employees", emp3, empPageAddress);
            
            List<Object> emp4 = new ArrayList<>();
            emp4.add(4);
            emp4.add("Diana");
            emp4.add(70000);
            emp4.add(4000);
            emp4.add("Marketing");
            empPageAddress = store.insertSingleRow("employees", emp4, empPageAddress);
            
            List<Object> emp5 = new ArrayList<>();
            emp5.add(5);
            emp5.add("Eve");
            emp5.add(90000);
            emp5.add(10000);
            emp5.add("Engineering");
            empPageAddress = store.insertSingleRow("employees", emp5, empPageAddress);
            
            System.out.println("Inserted 5 employee records\n");

            // ========== Create departments table ==========
            System.out.println("=== Creating Departments Table ===");
            List<Attribute> deptAttributes = new ArrayList<>();
            deptAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            deptAttributes.add(new Attribute("name", new VarCharDefinition(false, false, 20)));
            deptAttributes.add(new Attribute("budget", new DoubleDefinition(false, false)));
            
            TableSchema deptTable = new TableSchema("departments", deptAttributes);
            store.CreateTable(deptTable);
            System.out.println("Table 'departments' created successfully!");
            
            // Insert department data
            int deptPageAddress = catalog.getAddressOfPage("departments");
            
            List<Object> dept1 = new ArrayList<>();
            dept1.add(1);
            dept1.add("Engineering");
            dept1.add(500000.0);
            deptPageAddress = store.insertSingleRow("departments", dept1, deptPageAddress);
            
            List<Object> dept2 = new ArrayList<>();
            dept2.add(2);
            dept2.add("Sales");
            dept2.add(300000.0);
            deptPageAddress = store.insertSingleRow("departments", dept2, deptPageAddress);
            
            List<Object> dept3 = new ArrayList<>();
            dept3.add(3);
            dept3.add("Marketing");
            dept3.add(250000.0);
            deptPageAddress = store.insertSingleRow("departments", dept3, deptPageAddress);
            
            System.out.println("Inserted 3 department records\n");

            // ========== Create products table ==========
            System.out.println("=== Creating Products Table ===");
            List<Attribute> productsAttributes = new ArrayList<>();
            productsAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            productsAttributes.add(new Attribute("name", new VarCharDefinition(false, false, 50)));
            productsAttributes.add(new Attribute("price", new DoubleDefinition(false, false)));
            productsAttributes.add(new Attribute("quantity", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false)));
            productsAttributes.add(new Attribute("category", new VarCharDefinition(false, false, 20)));
            
            TableSchema productsTable = new TableSchema("products", productsAttributes);
            store.CreateTable(productsTable);
            System.out.println("Table 'products' created successfully!");
            
            // Insert product data
            int prodPageAddress = catalog.getAddressOfPage("products");
            
            List<Object> prod1 = new ArrayList<>();
            prod1.add(1);
            prod1.add("Laptop");
            prod1.add(999.99);
            prod1.add(50);
            prod1.add("Electronics");
            prodPageAddress = store.insertSingleRow("products", prod1, prodPageAddress);
            
            List<Object> prod2 = new ArrayList<>();
            prod2.add(2);
            prod2.add("Mouse");
            prod2.add(25.50);
            prod2.add(200);
            prod2.add("Electronics");
            prodPageAddress = store.insertSingleRow("products", prod2, prodPageAddress);
            
            List<Object> prod3 = new ArrayList<>();
            prod3.add(3);
            prod3.add("Desk");
            prod3.add(199.99);
            prod3.add(30);
            prod3.add("Furniture");
            prodPageAddress = store.insertSingleRow("products", prod3, prodPageAddress);
            
            List<Object> prod4 = new ArrayList<>();
            prod4.add(4);
            prod4.add("Chair");
            prod4.add(149.99);
            prod4.add(75);
            prod4.add("Furniture");
            prodPageAddress = store.insertSingleRow("products", prod4, prodPageAddress);
            
            List<Object> prod5 = new ArrayList<>();
            prod5.add(5);
            prod5.add("Monitor");
            prod5.add(299.99);
            prod5.add(40);
            prod5.add("Electronics");
            prodPageAddress = store.insertSingleRow("products", prod5, prodPageAddress);
            
            System.out.println("Inserted 5 product records\n");

            // ========== Create nullable_test table ==========
            System.out.println("=== Creating Nullable Test Table ===");
            List<Attribute> nullableAttributes = new ArrayList<>();
            nullableAttributes.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false)));
            nullableAttributes.add(new Attribute("value", new IntegerDefinition(AttributeTypeEnum.INTEGER, false, true)));
            nullableAttributes.add(new Attribute("text", new VarCharDefinition(false, true, 20)));
            
            TableSchema nullableTable = new TableSchema("nullable_test", nullableAttributes);
            store.CreateTable(nullableTable);
            System.out.println("Table 'nullable_test' created successfully!");
            
            // Insert data with NULLs
            int nullPageAddress = catalog.getAddressOfPage("nullable_test");
            
            List<Object> null1 = new ArrayList<>();
            null1.add(1);
            null1.add(10);
            null1.add("HasValue");
            nullPageAddress = store.insertSingleRow("nullable_test", null1, nullPageAddress);
            
            List<Object> null2 = new ArrayList<>();
            null2.add(2);
            null2.add(null);  // NULL value
            null2.add("NullValue");
            nullPageAddress = store.insertSingleRow("nullable_test", null2, nullPageAddress);
            
            List<Object> null3 = new ArrayList<>();
            null3.add(3);
            null3.add(30);
            null3.add(null);  // NULL text
            nullPageAddress = store.insertSingleRow("nullable_test", null3, nullPageAddress);
            
            List<Object> null4 = new ArrayList<>();
            null4.add(4);
            null4.add(null);  // NULL value
            null4.add(null);  // NULL text
            nullPageAddress = store.insertSingleRow("nullable_test", null4, nullPageAddress);
            
            System.out.println("Inserted 4 nullable test records\n");

            String separator = "======================================================================";
            
            System.out.println("=== Starting Comprehensive SELECT Tests ===\n");
            System.out.println(separator + "\n");

            // ========================================
            // SECTION 1: Basic Projection Tests
            // ========================================
            System.out.println("SECTION 1: BASIC PROJECTION TESTS");
            System.out.println(separator + "\n");

            // Test 1.1: SELECT *
            System.out.println("=== Test 1.1: SELECT * FROM employees ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees;");
                System.out.println("✓ Test 1.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 1.1 failed: " + e.getMessage() + "\n");
            }

            // Test 1.2: Single column projection
            System.out.println("=== Test 1.2: SELECT name FROM employees ===");
            try {
                ParserDML.runCommand("SELECT name FROM employees;");
                System.out.println("✓ Test 1.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 1.2 failed: " + e.getMessage() + "\n");
            }

            // Test 1.3: Multiple column projection
            System.out.println("=== Test 1.3: SELECT name, salary, department FROM employees ===");
            try {
                ParserDML.runCommand("SELECT name, salary, department FROM employees;");
                System.out.println("✓ Test 1.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 1.3 failed: " + e.getMessage() + "\n");
            }

            // Test 1.4: Projection with different order
            System.out.println("=== Test 1.4: SELECT salary, name, id FROM employees ===");
            try {
                ParserDML.runCommand("SELECT salary, name, id FROM employees;");
                System.out.println("✓ Test 1.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 1.4 failed: " + e.getMessage() + "\n");
            }

            // Test 1.5: Qualified column names
            System.out.println("=== Test 1.5: SELECT employees.name, employees.salary FROM employees ===");
            try {
                ParserDML.runCommand("SELECT employees.name, employees.salary FROM employees;");
                System.out.println("✓ Test 1.5 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 1.5 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 2: WHERE Clause - Relational Operators
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 2: WHERE CLAUSE - RELATIONAL OPERATORS");
            System.out.println(separator + "\n");

            // Test 2.1: Equals (=)
            System.out.println("=== Test 2.1: SELECT * FROM employees WHERE salary = 75000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary = 75000;");
                System.out.println("✓ Test 2.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.1 failed: " + e.getMessage() + "\n");
            }

            // Test 2.2: Greater than (>)
            System.out.println("=== Test 2.2: SELECT name, salary FROM employees WHERE salary > 70000 ===");
            try {
                ParserDML.runCommand("SELECT name, salary FROM employees WHERE salary > 70000;");
                System.out.println("✓ Test 2.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.2 failed: " + e.getMessage() + "\n");
            }

            // Test 2.3: Greater than or equal (>=)
            System.out.println("=== Test 2.3: SELECT * FROM employees WHERE salary >= 75000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary >= 75000;");
                System.out.println("✓ Test 2.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.3 failed: " + e.getMessage() + "\n");
            }

            // Test 2.4: Less than (<)
            System.out.println("=== Test 2.4: SELECT name, salary FROM employees WHERE salary < 75000 ===");
            try {
                ParserDML.runCommand("SELECT name, salary FROM employees WHERE salary < 75000;");
                System.out.println("✓ Test 2.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.4 failed: " + e.getMessage() + "\n");
            }

            // Test 2.5: Less than or equal (<=)
            System.out.println("=== Test 2.5: SELECT * FROM employees WHERE bonus <= 5000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE bonus <= 5000;");
                System.out.println("✓ Test 2.5 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.5 failed: " + e.getMessage() + "\n");
            }

            // Test 2.6: Not equal (<>)
            System.out.println("=== Test 2.6: SELECT name, department FROM employees WHERE department <> \"Engineering\" ===");
            try {
                ParserDML.runCommand("SELECT name, department FROM employees WHERE department <> \"Engineering\";");
                System.out.println("✓ Test 2.6 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.6 failed: " + e.getMessage() + "\n");
            }

            // Test 2.7: VARCHAR comparison
            System.out.println("=== Test 2.7: SELECT * FROM products WHERE category = \"Electronics\" ===");
            try {
                ParserDML.runCommand("SELECT * FROM products WHERE category = \"Electronics\";");
                System.out.println("✓ Test 2.7 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.7 failed: " + e.getMessage() + "\n");
            }

            // Test 2.8: DOUBLE comparison
            System.out.println("=== Test 2.8: SELECT name, price FROM products WHERE price > 150.0 ===");
            try {
                ParserDML.runCommand("SELECT name, price FROM products WHERE price > 150.0;");
                System.out.println("✓ Test 2.8 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 2.8 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 3: WHERE Clause - Logical Operators
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 3: WHERE CLAUSE - LOGICAL OPERATORS");
            System.out.println(separator + "\n");

            // Test 3.1: AND operator
            System.out.println("=== Test 3.1: SELECT * FROM employees WHERE salary > 70000 AND bonus > 5000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary > 70000 AND bonus > 5000;");
                System.out.println("✓ Test 3.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 3.1 failed: " + e.getMessage() + "\n");
            }

            // Test 3.2: OR operator
            System.out.println("=== Test 3.2: SELECT name, department FROM employees WHERE department = Sales OR department = Marketing ===");
            try {
                ParserDML.runCommand("SELECT name, department FROM employees WHERE department = \"Sales\" OR department = \"Marketing\";");
                System.out.println("✓ Test 3.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 3.2 failed: " + e.getMessage() + "\n");
            }

            // Test 3.3: Multiple AND conditions
            System.out.println("=== Test 3.3: SELECT * FROM products WHERE category = \"Electronics\" AND price < 300.0 AND quantity > 50 ===");
            try {
                ParserDML.runCommand("SELECT * FROM products WHERE category = \"Electronics\" AND price < 300.0 AND quantity > 50;");
                System.out.println("✓ Test 3.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 3.3 failed: " + e.getMessage() + "\n");
            }

            // Test 3.4: AND and OR combination (relational → AND → OR)
            System.out.println("=== Test 3.4: SELECT * FROM employees WHERE salary > 75000 OR department = \"Sales\" AND bonus > 2000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary > 75000 OR department = \"Sales\" AND bonus > 2000;");
                System.out.println("✓ Test 3.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 3.4 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 4: WHERE Clause - IS NULL
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 4: WHERE CLAUSE - IS NULL");
            System.out.println(separator + "\n");

            // Test 4.1: IS NULL check
            System.out.println("=== Test 4.1: SELECT * FROM nullable_test WHERE value IS NULL ===");
            try {
                ParserDML.runCommand("SELECT * FROM nullable_test WHERE value IS NULL;");
                System.out.println("✓ Test 4.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 4.1 failed: " + e.getMessage() + "\n");
            }

            // Test 4.2: IS NULL on VARCHAR
            System.out.println("=== Test 4.2: SELECT id, text FROM nullable_test WHERE text IS NULL ===");
            try {
                ParserDML.runCommand("SELECT id, text FROM nullable_test WHERE text IS NULL;");
                System.out.println("✓ Test 4.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 4.2 failed: " + e.getMessage() + "\n");
            }

            // Test 4.3: IS NULL with AND
            System.out.println("=== Test 4.3: SELECT * FROM nullable_test WHERE value IS NULL AND text IS NULL ===");
            try {
                ParserDML.runCommand("SELECT * FROM nullable_test WHERE value IS NULL AND text IS NULL;");
                System.out.println("✓ Test 4.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 4.3 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 5: Mathematical Operations in WHERE
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 5: MATHEMATICAL OPERATIONS IN WHERE");
            System.out.println(separator + "\n");

            // Test 5.1: Addition
            System.out.println("=== Test 5.1: SELECT name, salary, bonus FROM employees WHERE salary + bonus > 80000 ===");
            try {
                ParserDML.runCommand("SELECT name, salary, bonus FROM employees WHERE salary + bonus > 80000;");
                System.out.println("✓ Test 5.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 5.1 failed: " + e.getMessage() + "\n");
            }

            // Test 5.2: Subtraction
            System.out.println("=== Test 5.2: SELECT * FROM employees WHERE salary - bonus < 65000 ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary - bonus < 65000;");
                System.out.println("✓ Test 5.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 5.2 failed: " + e.getMessage() + "\n");
            }

            // Test 5.3: Multiplication
            System.out.println("=== Test 5.3: SELECT name, quantity FROM products WHERE quantity * 2 > 100 ===");
            try {
                ParserDML.runCommand("SELECT name, quantity FROM products WHERE quantity * 2 > 100;");
                System.out.println("✓ Test 5.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 5.3 failed: " + e.getMessage() + "\n");
            }

            // Test 5.4: Division (INTEGER)
            System.out.println("=== Test 5.4: SELECT name, salary FROM employees WHERE salary / 1000 > 70 ===");
            try {
                ParserDML.runCommand("SELECT name, salary FROM employees WHERE salary / 1000 > 70;");
                System.out.println("✓ Test 5.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 5.4 failed: " + e.getMessage() + "\n");
            }

            // Test 5.5: Math with DOUBLE
            System.out.println("=== Test 5.5: SELECT name, price FROM products WHERE price * 1.1 > 200.0 ===");
            try {
                ParserDML.runCommand("SELECT name, price FROM products WHERE price * 1.1 > 200.0;");
                System.out.println("✓ Test 5.5 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 5.5 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 6: ORDERBY Tests
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 6: ORDERBY TESTS");
            System.out.println(separator + "\n");

            // Test 6.1: ORDERBY INTEGER
            System.out.println("=== Test 6.1: SELECT * FROM employees ORDERBY salary ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees ORDERBY salary;");
                System.out.println("✓ Test 6.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 6.1 failed: " + e.getMessage() + "\n");
            }

            // Test 6.2: ORDERBY VARCHAR
            System.out.println("=== Test 6.2: SELECT name, department FROM employees ORDERBY name ===");
            try {
                ParserDML.runCommand("SELECT name, department FROM employees ORDERBY name;");
                System.out.println("✓ Test 6.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 6.2 failed: " + e.getMessage() + "\n");
            }

            // Test 6.3: ORDERBY DOUBLE
            System.out.println("=== Test 6.3: SELECT * FROM products ORDERBY price ===");
            try {
                ParserDML.runCommand("SELECT * FROM products ORDERBY price;");
                System.out.println("✓ Test 6.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 6.3 failed: " + e.getMessage() + "\n");
            }

            // Test 6.4: ORDERBY with qualified name
            System.out.println("=== Test 6.4: SELECT * FROM employees ORDERBY employees.bonus ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees ORDERBY employees.bonus;");
                System.out.println("✓ Test 6.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 6.4 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 7: Combined Operations
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 7: COMBINED OPERATIONS");
            System.out.println(separator + "\n");

            // Test 7.1: Projection + WHERE
            System.out.println("=== Test 7.1: SELECT name, salary FROM employees WHERE salary > 70000 ===");
            try {
                ParserDML.runCommand("SELECT name, salary FROM employees WHERE salary > 70000;");
                System.out.println("✓ Test 7.1 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.1 failed: " + e.getMessage() + "\n");
            }

            // Test 7.2: WHERE + ORDERBY
            System.out.println("=== Test 7.2: SELECT * FROM employees WHERE salary > 65000 ORDERBY salary ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE salary > 65000 ORDERBY salary;");
                System.out.println("✓ Test 7.2 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.2 failed: " + e.getMessage() + "\n");
            }

            // Test 7.3: Projection + ORDERBY
            System.out.println("=== Test 7.3: SELECT name, price FROM products ORDERBY price ===");
            try {
                ParserDML.runCommand("SELECT name, price FROM products ORDERBY price;");
                System.out.println("✓ Test 7.3 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.3 failed: " + e.getMessage() + "\n");
            }

            // Test 7.4: Projection + WHERE + ORDERBY
            System.out.println("=== Test 7.4: SELECT name, salary, department FROM employees WHERE department = \"Engineering\" ORDERBY salary ===");
            try {
                ParserDML.runCommand("SELECT name, salary, department FROM employees WHERE department = \"Engineering\" ORDERBY salary;");
                System.out.println("✓ Test 7.4 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.4 failed: " + e.getMessage() + "\n");
            }

            // Test 7.5: WHERE with math + ORDERBY
            System.out.println("=== Test 7.5: SELECT name, salary, bonus FROM employees WHERE salary + bonus > 75000 ORDERBY salary ===");
            try {
                ParserDML.runCommand("SELECT name, salary, bonus FROM employees WHERE salary + bonus > 75000 ORDERBY salary;");
                System.out.println("✓ Test 7.5 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.5 failed: " + e.getMessage() + "\n");
            }

            // Test 7.6: Complex WHERE with AND/OR + ORDERBY
            System.out.println("=== Test 7.6: SELECT * FROM products WHERE category = Electronics OR price < 150.0 ORDERBY price ===");
            try {
                ParserDML.runCommand("SELECT * FROM products WHERE category = Electronics OR price < 150.0 ORDERBY price;");
                System.out.println("✓ Test 7.6 passed\n");
            } catch (Exception e) {
                System.out.println("✗ Test 7.6 failed: " + e.getMessage() + "\n");
            }

            // ========================================
            // SECTION 8: Error Cases
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 8: ERROR CASES");
            System.out.println(separator + "\n");

            // Error Test 1: Non-existent column in SELECT
            System.out.println("=== Error Test 8.1: SELECT invalid_column FROM employees (should fail) ===");
            try {
                ParserDML.runCommand("SELECT invalid_column FROM employees;");
                System.out.println("✗ ERROR: Should have thrown an exception!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            // Error Test 2: Non-existent column in WHERE
            System.out.println("=== Error Test 8.2: SELECT * FROM employees WHERE invalid_col = 5 (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees WHERE invalid_col = 5;");
                System.out.println("✗ ERROR: Should have thrown an exception!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            // Error Test 3: Non-existent column in ORDERBY
            System.out.println("=== Error Test 8.3: SELECT * FROM employees ORDERBY invalid_col (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM employees ORDERBY invalid_col;");
                System.out.println("✗ ERROR: Should have thrown an exception!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            // Error Test 4: Non-existent table
            System.out.println("=== Error Test 8.4: SELECT * FROM nonexistent_table (should fail) ===");
            try {
                ParserDML.runCommand("SELECT * FROM nonexistent_table;");
                System.out.println("✗ ERROR: Should have thrown an exception!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            // Error Test 5: Wrong table qualifier
            System.out.println("=== Error Test 8.5: SELECT products.name FROM employees (should fail) ===");
            try {
                ParserDML.runCommand("SELECT products.name FROM employees;");
                System.out.println("✗ ERROR: Should have thrown an exception!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            // ========================================
            // SECTION 9: Cartesian Product Tests (if implemented)
            // ========================================
            System.out.println("\n" + separator);
            System.out.println("SECTION 9: CARTESIAN PRODUCT TESTS (if FROM supports multiple tables)");
            System.out.println(separator + "\n");

            // Note: These tests will only work once cartesian product is implemented in fromParse()
            
            System.out.println("=== Test 9.1: SELECT * FROM employees, departments ===");
            System.out.println("(Will fail if cartesian product not yet implemented)");
            try {
                ParserDML.runCommand("SELECT * FROM employees, departments;");
                System.out.println("✓ Test 9.1 passed\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("⚠ Test 9.1: " + message + "\n");
            }

            System.out.println("=== Test 9.2: SELECT employees.name, departments.name FROM employees, departments ===");
            System.out.println("(Requires qualified names due to ambiguous 'name')");
            try {
                ParserDML.runCommand("SELECT employees.name, departments.name FROM employees, departments;");
                System.out.println("✓ Test 9.2 passed\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("⚠ Test 9.2: " + message + "\n");
            }

            System.out.println("=== Test 9.3: SELECT * FROM employees, departments WHERE employees.department = departments.name ===");
            System.out.println("(JOIN condition using WHERE)");
            try {
                ParserDML.runCommand("SELECT * FROM employees, departments WHERE employees.department = departments.name;");
                System.out.println("✓ Test 9.3 passed\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("⚠ Test 9.3: " + message + "\n");
            }

            System.out.println("=== Error Test 9.4: SELECT name FROM employees, departments (should fail - ambiguous) ===");
            try {
                ParserDML.runCommand("SELECT name FROM employees, departments;");
                System.out.println("✗ ERROR: Should have thrown an exception for ambiguous 'name'!\n");
            } catch (Exception e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ Correctly caught error: " + message + "\n");
            }

            System.out.println("\n" + separator);
            System.out.println("=== All Tests Completed! ===");
            System.out.println(separator);

        } catch (SQLSyntaxErrorException e) {
            System.err.println("SQL Error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
