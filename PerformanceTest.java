import StorageManager.StorageManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Logger;
import DMLParser.ParserDML;
import java.util.*;
import java.io.File;

/**
 * Performance test to compare insertion speed with indexing ON vs OFF
 */
public class PerformanceTest {
    
    // Configure the number of records to insert
    private static final int NUM_RECORDS = 100000;
    
    // Configure which tests to run
    private static final boolean RUN_WITHOUT_INDEXING = false;
    private static final boolean RUN_WITH_INDEXING = true;
    
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
     * @param startId starting ID value
     * @param count number of rows to generate
     * @return complete INSERT command string
     */
    private static String buildBatchInsertCommand(String tableName, int startId, int count) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("INSERT ").append(tableName).append(" VALUES (");
        
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                cmd.append(",");
            }
            
            int id = startId + i;
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
        
        System.out.println("\n========================================");
        System.out.println("B+ TREE INDEXING PERFORMANCE TEST");
        System.out.println("========================================");
        System.out.println("Records to insert: " + NUM_RECORDS);
        System.out.println("Table schema: id INT (PK), value DOUBLE, name VARCHAR(5)");
        System.out.println();

        long timeWithoutIndexing = 0;
        long timeWithIndexing = 0;

        // ============================================================
        // TEST 1: Insert WITHOUT indexing
        // ============================================================
        if (RUN_WITHOUT_INDEXING) {
            System.out.println("TEST 1: Insertion WITHOUT B+ tree indexing");
            System.out.println("--------------------");
            try {
                cleanupDatabase("perftest_noindex");
                StorageManager.initDatabase("perftest_noindex", 1098, 5, false); // indexing = false
                StorageManager store = StorageManager.getStorageManager();
                
                // Create table
                List<Attribute> attrs = new ArrayList<>();
                attrs.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
                attrs.add(new Attribute("value", new DoubleDefinition(false, false, false), null));
                attrs.add(new Attribute("name", new VarCharDefinition(false, false, 5, false), null));
                
                TableSchema table = new TableSchema("test_table_noindex", attrs);
                store.CreateTable(table);
                
                // Build batch INSERT command and measure time
                System.out.println("  Building batch INSERT command...");
                String batchInsert = buildBatchInsertCommand("test_table_noindex", 1, NUM_RECORDS);
                System.out.println("  Executing batch INSERT with " + NUM_RECORDS + " rows...");
                
                long startTime = System.currentTimeMillis();
                ParserDML.runCommand(batchInsert);
                long endTime = System.currentTimeMillis();
                timeWithoutIndexing = endTime - startTime;
                
                System.out.println("✓ Inserted " + NUM_RECORDS + " records");
                System.out.println("  Time: " + timeWithoutIndexing + " ms");
                System.out.println("  Average: " + String.format("%.3f", (double)timeWithoutIndexing / NUM_RECORDS) + " ms/record");
                
                store.shutdown();
                
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // Small pause between tests
            System.out.println();
        } else {
            System.out.println("TEST 1: SKIPPED (RUN_WITHOUT_INDEXING = false)");
            System.out.println();
        }

        // ============================================================
        // TEST 2: Insert WITH indexing
        // ============================================================
        if (RUN_WITH_INDEXING) {
            System.out.println("TEST 2: Insertion WITH B+ tree indexing");
            System.out.println("--------------------");
            try {
                cleanupDatabase("perftest_index");
                StorageManager.initDatabase("perftest_index", 1098, 5, true); // indexing = true
                StorageManager store = StorageManager.getStorageManager();
                
                // Create table (same schema)
                List<Attribute> attrs = new ArrayList<>();
                attrs.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
                attrs.add(new Attribute("value", new DoubleDefinition(false, false, false), null));
                attrs.add(new Attribute("name", new VarCharDefinition(false, false, 5, false), null));
                
                TableSchema table = new TableSchema("test_table_index", attrs);
                store.CreateTable(table);
                
                // Build batch INSERT command and measure time
                System.out.println("  Building batch INSERT command...");
                String batchInsert = buildBatchInsertCommand("test_table_index", 1, NUM_RECORDS);
                System.out.println("  Executing batch INSERT with " + NUM_RECORDS + " rows...");
                
                long startTime = System.currentTimeMillis();
                ParserDML.runCommand(batchInsert);
                long endTime = System.currentTimeMillis();
                timeWithIndexing = endTime - startTime;
                
                System.out.println("✓ Inserted " + NUM_RECORDS + " records");
                System.out.println("  Time: " + timeWithIndexing + " ms");
                System.out.println("  Average: " + String.format("%.3f", (double)timeWithIndexing / NUM_RECORDS) + " ms/record");
                
                store.shutdown();
                
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("TEST 2: SKIPPED (RUN_WITH_INDEXING = false)");
        }

        // ============================================================
        // PERFORMANCE COMPARISON
        // ============================================================
        if (RUN_WITHOUT_INDEXING && RUN_WITH_INDEXING) {
            System.out.println("\n========================================");
            System.out.println("PERFORMANCE COMPARISON");
            System.out.println("========================================");
            System.out.println("Without indexing: " + timeWithoutIndexing + " ms");
            System.out.println("With indexing:    " + timeWithIndexing + " ms");
            
            if (timeWithoutIndexing > 0 && timeWithIndexing > 0) {
                long difference = Math.abs(timeWithIndexing - timeWithoutIndexing);
                double percentDiff = ((double)difference / timeWithoutIndexing) * 100;
                
                System.out.println("Difference:       " + difference + " ms");
                
                if (timeWithIndexing < timeWithoutIndexing) {
                    System.out.println("Result:           B+ tree indexing is " + 
                        String.format("%.1f", percentDiff) + "% FASTER");
                } else if (timeWithIndexing > timeWithoutIndexing) {
                    System.out.println("Result:           B+ tree indexing is " + 
                        String.format("%.1f", percentDiff) + "% SLOWER");
                } else {
                    System.out.println("Result:           Equal performance");
                }
            }
            System.out.println("========================================\n");
        } else if (RUN_WITHOUT_INDEXING) {
            System.out.println("\n========================================");
            System.out.println("TEST RESULTS");
            System.out.println("========================================");
            System.out.println("Without indexing: " + timeWithoutIndexing + " ms");
            System.out.println("========================================\n");
        } else if (RUN_WITH_INDEXING) {
            System.out.println("\n========================================");
            System.out.println("TEST RESULTS");
            System.out.println("========================================");
            System.out.println("With indexing:    " + timeWithIndexing + " ms");
            System.out.println("========================================\n");
        }
    }
}
