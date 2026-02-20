package DMLParser;

import Common.Command;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.Attribute;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.ArrayList;

public class Select implements Command{

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException{
        // for right now select command is simple
        // SELECT * FROM <table>;

        if (command.length < 4){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        // Check keywords (case-insensitive)
        if (!command[0].equalsIgnoreCase("select") || 
            !command[1].equals("*") || 
            !command[2].equalsIgnoreCase("from")){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        // Table name - preserve case as provided
        String tableName = command[3].toLowerCase();

        // Verify that table exists
        Catalog cat = Catalog.getInstance();
        if (!cat.tableExists(tableName)){
            throw new SQLSyntaxErrorException(
                    "Table does not exist: " + tableName
            );
        }

        // Run the actual select command
        Logger.log("Running SELECT * FROM " + tableName);
        StorageManager store = StorageManager.getStorageManager();
        try {
            Page currentPage = store.selectFirstPage(tableName);
            
            if (currentPage == null) {
                // Empty table
                Logger.log("Table " + tableName + " is empty");
                return true;
            }
            
            // Get table schema for column names
            TableSchema schema = cat.getTable(tableName);
            List<Attribute> attributes = schema.getAttributes();
            
            // Collect all rows first to calculate column widths
            List<List<Object>> allRows = new ArrayList<>();
            while (true){
                for (int i = 0; i < currentPage.getNumRows(); i++){
                    allRows.add(currentPage.getRecord(i));
                }
                if (currentPage.getNextPage() == -1){
                    break;
                }
                currentPage = store.select(currentPage.getNextPage(), tableName);
            }
            
            // Calculate column widths
            int[] columnWidths = new int[attributes.size()];
            for (int i = 0; i < attributes.size(); i++) {
                // Start with header name length
                columnWidths[i] = attributes.get(i).getName().length();
                
                // Check all data values
                for (List<Object> row : allRows) {
                    String value = formatValue(row.get(i));
                    columnWidths[i] = Math.max(columnWidths[i], value.length());
                }
                
                // Minimum width of 4 for readability
                columnWidths[i] = Math.max(columnWidths[i], 4);
            }
            
            // Print header row
            for (int i = 0; i < attributes.size(); i++) {
                System.out.print("|");
                System.out.print(String.format(" %" + columnWidths[i] + "s ", 
                    attributes.get(i).getName()));
            }
            System.out.println("|");
            
            // Print separator line
            for (int i = 0; i < attributes.size(); i++) {
                System.out.print("-");
                for (int j = 0; j < columnWidths[i] + 2; j++) {
                    System.out.print("-");
                }
            }
            System.out.println("-");
            
            // Print data rows
            for (List<Object> row : allRows) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print("|");
                    String value = formatValue(row.get(i));
                    System.out.print(String.format(" %" + columnWidths[i] + "s ", value));
                }
                System.out.println("|");
            }

        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error reading from table: " + e.getMessage());
        }

        return true;
    }
    
    /**
     * Formats a value for display in the table
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "True" : "False";
        }
        if (value instanceof String && ((String) value).isEmpty()) {
            return "";
        }
        return value.toString();
    }

}
