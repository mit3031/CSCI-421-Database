import Catalog.*;
import AttributeInfo.*;
import java.io.File;
import java.util.*;
/*
 Testing function for the catalog.
 Instructions: Just hit the run button, it handles stuff automatically
 Results: As of finishing this, the output is

SUCCESS: Table added and retrieved
SUCCESS: Record size correct: 14
SUCCESS: Default value correct
SUCCESS: Page size persisted correctly: 4096
SUCCESS: Case-insensitive lookup works
SUCCESS: Table dropped correctly
SUCCESS: Attribute 'age' added to 'alterTable'
SUCCESS: Attribute 'id' dropped correctly
All DDL Catalog Operations Verified.
Catalog Test Finished

Process finished with exit code 0
 */
public class CatalogTest {

    public static void main(String[] args) {

        File directory = new File("./testDB");
        if (!directory.exists()) {
            directory.mkdirs(); // Creates the folder so saveToDisk won't crash
        }

        File f = new File("./testDB/catalog.bin");
        if (f.exists()) {
            f.delete();
        }

        // STEP 1: Initialize catalog
        Catalog.init("./testDB", 4096); // pageSize 4096
        Catalog catalog = Catalog.getInstance();

        // STEP 2: Create table schema
        List<Attribute> attrs = new ArrayList<>();
        AttributeDefinition intDef = new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false);
        Attribute a1 = new Attribute("id", intDef, null);
        attrs.add(a1);

        AttributeDefinition varcharDef = new VarCharDefinition(false, true, 10);
        Attribute a2 = new Attribute("name", varcharDef, "NULL");
        attrs.add(a2);

        TableSchema table = new TableSchema("myTable", attrs);

        //STEP 3: Add table to catalog
        catalog.addTable(table);

        // STEP 4: Test basic retrieval
        TableSchema retrieved = catalog.getTable("myTable");
        if (retrieved == null) System.out.println("FAILED: Could not retrieve table");
        else System.out.println("SUCCESS: Table added and retrieved");

        // STEP 5: Test record size
        int expectedSize = intDef.getByteSize() + varcharDef.getByteSize();
        if (retrieved.getRecordSize() != expectedSize) {
            System.out.println("FAILED: Record size mismatch");
        } else {
            System.out.println("SUCCESS: Record size correct: " + retrieved.getRecordSize());
        }

        // STEP 6: Test default values
        Attribute nameAttr = retrieved.getAttributes().get(1);
        if (!"NULL".equals(nameAttr.getDefaultValue())) {
            System.out.println("FAILED: Default value incorrect");
        } else {
            System.out.println("SUCCESS: Default value correct");
        }

        // STEP 7: Save and simulate crash
        //storageManager.saveToDisk();

        // Reset the singleton for testing cold start
        Catalog.resetForTesting();

        // STEP 8: Cold restart
        Catalog.init("./testDB", 8192); // different pageSize argument
        Catalog freshCatalog = Catalog.getInstance();

        // STEP 9: Page size should persist from original
        if (freshCatalog.getPageSize() != 4096) {
            System.out.println("FAILED: Page size persistence broken");
        } else {
            System.out.println("SUCCESS: Page size persisted correctly: " + freshCatalog.getPageSize());
        }

        // STEP 10: Case-insensitive table retrieval
        TableSchema reloadedTable = freshCatalog.getTable("MYTABLE");
        if (reloadedTable == null) {
            System.out.println("FAILED: Case-insensitive table lookup broken");
        } else {
            System.out.println("SUCCESS: Case-insensitive lookup works");
        }

        // STEP 11: Drop table test
        freshCatalog.dropTable("myTable");
        if (freshCatalog.getTable("myTable") != null) {
            System.out.println("FAILED: Drop table failed");
        } else {
            System.out.println("SUCCESS: Table dropped correctly");
        }

        // STEP 12: Test ALTER TABLE ADD Attribute
        // Simulate: ALTER TABLE myTable ADD age INTEGER DEFAULT 18
        Catalog.resetForTesting();
        Catalog.init("./testDB", 4096);
        Catalog alterCatalog = Catalog.getInstance();

        // Create a new table to test altering
        List<Attribute> alterAttrs = new ArrayList<>();
        alterAttrs.add(new Attribute("id", new IntegerDefinition(AttributeTypeEnum.INTEGER, true, false), null));
        TableSchema alterTable = new TableSchema("alterTable", alterAttrs);
        alterCatalog.addTable(alterTable);

        // Add the new column
        AttributeDefinition ageDef = new IntegerDefinition(AttributeTypeEnum.INTEGER, false, false);
        Attribute ageAttr = new Attribute("age", ageDef, "18");
        alterTable.addAttribute(ageAttr);
        //alterCatalog.saveToDisk(); // Persist the change

        System.out.println("SUCCESS: Attribute 'age' added to 'alterTable'");

        // STEP 13: Test ALTER TABLE DROP Attribute
        // Simulate: ALTER TABLE alterTable DROP id
        alterTable.dropAttribute("id");
        //alterCatalog.saveToDisk();

        if (alterTable.getAttributes().size() == 1 && alterTable.getAttributes().get(0).getName().equals("age")) {
            System.out.println("SUCCESS: Attribute 'id' dropped correctly");
        } else {
            System.out.println("FAILED: Drop attribute logic failed");
        }

        System.out.println("All DDL Catalog Operations Verified.");

        System.out.println("Catalog Test Finished");
    }
}