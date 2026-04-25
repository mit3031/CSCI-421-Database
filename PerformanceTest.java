import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import DMLParser.ParserDML;
import java.util.*;
import java.io.File;

/**
 * Performance test for batch insertion with configurable indexing
 */
public class PerformanceTest {
    
    // ============================================================
    // TEST CONFIGURATION
    // ============================================================
    
    // Configure the number of records to insert
    private static final int NUM_RECORDS = 2000;
    
    // Configure whether B+ tree indexing is enabled
    private static final boolean INDEXING_ENABLED = true;
    
    // Configure batch size (smaller = less memory, more operations)
    private static final int BATCH_SIZE = 100;
    
    // Configure whether to verify primary key ordering with SELECT * after insertion
    private static final boolean VERIFY_ORDER = false;
    
    // Configure whether to randomize insertion order (useful for testing ordering)
    private static final boolean RANDOMIZE_INSERTION = false;
    
    /**
     * Recursively deletes a directory and all its contents
     */
    private static void cleanupDatabase(String dbPath) {
        File dbDir = new File(dbPath);
        if (dbDir.exists()) {
            try {
                deleteDirectory(dbDir);
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
    
    /**
     * Builds a batch INSERT command with multiple rows
     * @param tableName name of the table to insert into
     * @param ids list of IDs to insert
     * @return complete INSERT command string
     */
    private static String buildBatchInsertCommand(String tableName, List<Integer> ids) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("INSERT ").append(tableName).append(" VALUES (");
        
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                cmd.append(",");
            }
            
            int id = ids.get(i);
            double value = id * 1.5;
            String name = "n" + (id % 1000);
            
            // Format: id value "name"
            cmd.append(id).append(" ")
               .append(value).append(" ")
               .append("\"").append(name).append("\"");
        }
        
        cmd.append(");");
        return cmd.toString();
    }
    
    public static void main(String[] args) {
        String[] debugActive = new String[]{};
        Logger.initDebug(debugActive);
        
        // Determine test configuration
        String dbPath = INDEXING_ENABLED ? "perftest_index" : "perftest_noindex";
        String tableName = INDEXING_ENABLED ? "test_table_index" : "test_table_noindex";
        String indexingStatus = INDEXING_ENABLED ? "ENABLED" : "DISABLED";
        
        System.out.println("\n========================================");
        System.out.println("B+ TREE INDEXING PERFORMANCE TEST");
        System.out.println("========================================");
        System.out.println("Records to insert: " + NUM_RECORDS);
        System.out.println("B+ Tree Indexing:  " + indexingStatus);
        System.out.println("Table schema:      id INT (PK), value DOUBLE, name VARCHAR(5)");
        System.out.println("========================================\n");

        try {
            // Clean up and initialize database
            cleanupDatabase(dbPath);
            StorageManager.initDatabase(dbPath, 1098, 5, INDEXING_ENABLED);
            StorageManager store = StorageManager.getStorageManager();
            
            // Create table
            List<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
            attrs.add(new Attribute("value", new DoubleDefinition(false, false, false), null));
            attrs.add(new Attribute("name", new VarCharDefinition(false, false, 5, false), null));
            
            TableSchema table = new TableSchema(tableName, attrs);
            store.CreateTable(table);
            
            // Generate list of IDs to insert
            List<Integer> allIds = new ArrayList<>();
            for (int i = 1; i <= NUM_RECORDS; i++) {
                allIds.add(i);
            }
            
            // Randomize insertion order if requested
            if (RANDOMIZE_INSERTION) {
                Collections.shuffle(allIds);
                System.out.println("Insertion order: RANDOMIZED");
            } else {
                System.out.println("Insertion order: SEQUENTIAL");
            }
            
            // Insert records in batches to reduce memory usage
            System.out.println("Inserting " + NUM_RECORDS + " rows in batches of " + BATCH_SIZE + "...\n");
            
            long startTime = System.currentTimeMillis();
            
            int recordsInserted = 0;
            while (recordsInserted < NUM_RECORDS) {
                int batchSize = Math.min(BATCH_SIZE, NUM_RECORDS - recordsInserted);
                List<Integer> batchIds = allIds.subList(recordsInserted, recordsInserted + batchSize);
                String batchInsert = buildBatchInsertCommand(tableName, batchIds);
                ParserDML.runCommand(batchInsert);
                recordsInserted += batchSize;
                
                // Progress indicator
                if (recordsInserted % (BATCH_SIZE * 5) == 0 || recordsInserted == NUM_RECORDS) {
                    System.out.println("  Inserted " + recordsInserted + " / " + NUM_RECORDS + " records...");
                }
            }
            
            long endTime = System.currentTimeMillis();
            long elapsed = endTime - startTime;
            
            // Verify primary key ordering if requested
            if (VERIFY_ORDER) {
                System.out.println("\n========================================");
                System.out.println("VERIFYING PRIMARY KEY ORDER");
                System.out.println("========================================");
                String selectQuery = "SELECT * FROM " + tableName + ";";
                System.out.println("Running: " + selectQuery);
                System.out.println();
                
                ParserDML.runCommand(selectQuery);
                
            }
            
            // Display results
            System.out.println("\n========================================");
            System.out.println("TEST RESULTS");
            System.out.println("========================================");
            System.out.println("✓ Successfully inserted " + NUM_RECORDS + " records");
            System.out.println("  Total time:       " + elapsed + " ms");
            System.out.println("  Average per row:  " + String.format("%.3f", (double)elapsed / NUM_RECORDS) + " ms/record");
            System.out.println("  Throughput:       " + String.format("%.0f", (NUM_RECORDS * 1000.0) / elapsed) + " records/second");
            System.out.println("========================================\n");
            
            store.shutdown();
            
        } catch (Exception e) {
            System.out.println("\n✗ TEST FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
