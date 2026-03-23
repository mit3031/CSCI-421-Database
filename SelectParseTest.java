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
 * Creates a simple table using StorageManager, inserts data directly, and tests various SELECT queries
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
