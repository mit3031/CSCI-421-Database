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
    private static final int NUM_RECORDS = 1000;
    
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
    
    public static void main(String[] args) {
        String[] debugActive = new String[]{"--debug"};
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
        System.out.println("TEST 1: Insertion WITHOUT B+ tree indexing");
        System.out.println("--------------------");
        try {
            cleanupDatabase("perftest_noindex");
            StorageManager.initDatabase("perftest_noindex", 400, 5, false); // indexing = false
            StorageManager store = StorageManager.getStorageManager();
            
            // Create table
            List<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
            attrs.add(new Attribute("value", new DoubleDefinition(false, false, false), null));
            attrs.add(new Attribute("name", new VarCharDefinition(false, false, 5, false), null));
            
            TableSchema table = new TableSchema("test_table_noindex", attrs);
            store.CreateTable(table);
            
            // Insert records and measure time
            long startTime = System.currentTimeMillis();
            for (int i = 1; i <= NUM_RECORDS; i++) {
                double value = i * 1.5;
                String name = "n" + (i % 1000); // Simple string pattern
                ParserDML.runCommand("INSERT test_table_noindex VALUES (" + i + " " + value + " \"" + name + "\");");
            }
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

        // ============================================================
        // TEST 2: Insert WITH indexing
        // ============================================================
        System.out.println("TEST 2: Insertion WITH B+ tree indexing");
        System.out.println("--------------------");
        try {
            cleanupDatabase("perftest_index");
            StorageManager.initDatabase("perftest_index", 400, 10, true); // indexing = true
            StorageManager store = StorageManager.getStorageManager();
            
            // Create table (same schema)
            List<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("id", new IntegerDefinition(null, true, false, false), null));
            attrs.add(new Attribute("value", new DoubleDefinition(false, false, false), null));
            attrs.add(new Attribute("name", new VarCharDefinition(false, false, 5, false), null));
            
            TableSchema table = new TableSchema("test_table_index", attrs);
            store.CreateTable(table);
            
            // Insert records and measure time
            long startTime = System.currentTimeMillis();
            for (int i = 1; i <= NUM_RECORDS; i++) {
                double value = i * 1.5;
                String name = "n" + (i % 1000); // Same string pattern
                ParserDML.runCommand("INSERT test_table_index VALUES (" + i + " " + value + " \"" + name + "\");");
            }
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

        // ============================================================
        // PERFORMANCE COMPARISON
        // ============================================================
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
    }
}
