import StorageManager.StorageManager;
import StorageManager.BufferManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.Attribute;
import AttributeInfo.IntegerDefinition;
import Common.Page;
import Common.Logger;
import java.util.*;

public class Test20Rows {
    public static void main(String[] args) {
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        
        try {
            StorageManager.initDatabase("test20db", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("\n=== Testing 20 Row Insert ===\n");
            
            // Create table
            List<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("num", new IntegerDefinition(null, true, false), null));
            attrs.add(new Attribute("doubled", new IntegerDefinition(null, false, false), null));
            
            TableSchema table = new TableSchema("Test20", attrs);
            store.CreateTable(table);
            
            // Insert 20 rows
            List<List<Object>> rows = new ArrayList<>();
            for (int i = 1; i <= 20; i++) {
                rows.add(Arrays.asList(i, i * 2));
            }
            
            System.out.println("Inserting 20 rows...");
            store.insert("Test20", rows);
            System.out.println("Insert complete!\n");
            
            // Read back
            System.out.println("Reading back data:");
            Page page = store.selectFirstPage("Test20");
            int totalRows = 0;
            int pageNum = 1;
            
            while (page != null) {
                System.out.println("Page " + pageNum + ": " + page.getNumRows() + " rows");
                for (int i = 0; i < page.getNumRows(); i++) {
                    totalRows++;
                    ArrayList<Object> rec = page.getRecord(i);
                    if (rec.size() == 2 && rec.get(0) != null && rec.get(1) != null) {
                        int num = (int) rec.get(0);
                        int doubled = (int) rec.get(1);
                        System.out.println("  Row " + totalRows + ": num=" + num + ", doubled=" + doubled + 
                                         (doubled == num * 2 ? " ✓" : " ✗ WRONG"));
                    } else {
                        System.out.println("  Row " + totalRows + ": ERROR - " + rec);
                    }
                }
                
                if (page.getNextPage() != -1) {
                    page = store.select(page.getNextPage(), "Test20");
                    pageNum++;
                } else {
                    break;
                }
            }
            
            System.out.println("\n=== Summary ===");
            System.out.println("Total rows retrieved: " + totalRows);
            System.out.println("Expected: 20");
            System.out.println(totalRows == 20 ? "✓✓✓ SUCCESS!" : "✗✗✗ FAILED");
            
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
