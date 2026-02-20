import StorageManager.StorageManager;
import StorageManager.BufferManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import Common.Page;
import Common.Logger;
import java.util.*;

public class Test200Rows {
    
    public static void main(String[] args) {
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        
        try {
            StorageManager.initDatabase("test200db", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("\n=== Testing 200 Row Insert ===\n");
            
            // Create table
            List<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
            attrs.add(new Attribute("value", new IntegerDefinition(null, false, false), null));
            attrs.add(new Attribute("squared", new IntegerDefinition(null, false, false), null));
            
            TableSchema table = new TableSchema("Test200", attrs);
            store.CreateTable(table);
            
            // Insert 200 rows
            List<List<Object>> rows = new ArrayList<>();
            for (int i = 1; i <= 200; i++) {
                rows.add(Arrays.asList(i, i * 10, i * i));
            }
            
            System.out.println("Inserting 200 rows...");
            long startTime = System.currentTimeMillis();
            store.insert("Test200", rows);
            long endTime = System.currentTimeMillis();
            System.out.println("Insert complete in " + (endTime - startTime) + "ms\n");
            
            // Read back
            System.out.println("Reading back data:");
            Page page = store.selectFirstPage("Test200");
            int totalRows = 0;
            int pageNum = 1;
            int errorCount = 0;
            boolean showDetails = true; // Set to false to only show summary
            
            while (page != null) {
                System.out.println("Page " + pageNum + ": " + page.getNumRows() + " rows");
                
                for (int i = 0; i < page.getNumRows(); i++) {
                    totalRows++;
                    ArrayList<Object> rec = page.getRecord(i);
                    
                    if (rec.size() == 3 && rec.get(0) != null && rec.get(1) != null && rec.get(2) != null) {
                        int id = (int) rec.get(0);
                        int value = (int) rec.get(1);
                        int squared = (int) rec.get(2);
                        
                        boolean correct = (value == id * 10) && (squared == id * id);
                        
                        if (showDetails && totalRows <= 10) {
                            // Show first 10 rows
                            System.out.println("  Row " + totalRows + ": id=" + id + ", value=" + value + ", squared=" + squared + 
                                             (correct ? " ✓" : " ✗ WRONG"));
                        } else if (showDetails && totalRows > 190) {
                            // Show last 10 rows
                            System.out.println("  Row " + totalRows + ": id=" + id + ", value=" + value + ", squared=" + squared + 
                                             (correct ? " ✓" : " ✗ WRONG"));
                        } else if (totalRows == 11 && showDetails) {
                            System.out.println("  ... (rows 11-190 omitted for brevity) ...");
                        }
                        
                        if (!correct) errorCount++;
                    } else {
                        System.out.println("  Row " + totalRows + ": ERROR - " + rec);
                        errorCount++;
                    }
                }
                
                if (page.getNextPage() != -1) {
                    page = store.select(page.getNextPage(), "Test200");
                    pageNum++;
                } else {
                    break;
                }
            }
            
            System.out.println("\n=== Summary ===");
            System.out.println("Total rows retrieved: " + totalRows);
            System.out.println("Expected: 200");
            System.out.println("Pages used: " + pageNum);
            System.out.println("Errors found: " + errorCount);
            System.out.println("Avg rows per page: " + (totalRows / (double)pageNum));
            
            if (totalRows == 200 && errorCount == 0) {
                System.out.println("\n✓✓✓ ALL 200 ROWS STORED AND RETRIEVED CORRECTLY! ✓✓✓");
                System.out.println("Page splitting is working perfectly!");
            } else {
                System.out.println("\n✗✗✗ TEST FAILED ✗✗✗");
                System.out.println("Expected 200 rows with 0 errors, got " + totalRows + " rows with " + errorCount + " errors");
            }
            
        } catch (Exception e) {
            System.out.println("\n✗✗✗ ERROR ✗✗✗");
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
