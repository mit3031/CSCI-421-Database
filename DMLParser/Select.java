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
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;

/*

General format for SELECT commands

SELECT a,b,c // 4th operation
FROM t1,t2 // 1st operation
WHERE a=5 and d>2 // 2nd operation 
ordering by d; // 3rd operation

General flow:

parseSelect(String[] command):

1. Split (already assumed to be done so by ParserDML)

2. from(from pieces) --> returns new table name

3. where(table name) --> returns new table name

4. order_by(table name) --> returns new table name

5. select(table_name) --> returns nothing

6. go back and remove all the temp tables



*/


public class Select implements Command{

    private String extractMiddleSection(String str, String start, String end){
        // 1. Find the index of the start string and add its length
        int startIndex = str.indexOf(start) + start.length();
        
        // 2. Find the index of the end string
        int endIndex = str.indexOf(end);

        return str.substring(startIndex, endIndex);
    }

    private String parseFrom(String fromPieces) throws SQLSyntaxErrorException{
        List<String> fromSplit = Arrays.stream(fromPieces.split(",")).map(String::trim).collect(Collectors.toList());

        Logger.log("Tables Detected: " + fromSplit);

        // go through each table and check that they exists
        for (int i = 0; i < fromSplit.size(); i++){
            String table = fromSplit.get(i);
            Catalog catalog = Catalog.getInstance();
            
            if (!catalog.tableExists(table)){
                throw new SQLSyntaxErrorException("Table: " + table + " does not exists");
            }
        }

        // TODO: get the cartesian product of all the tables
        // Storage

        return "this is the new table name"; 
    }

    public boolean parseSelect(String[] command) throws SQLSyntaxErrorException{
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < command.length; i++) {
            sb.append(command[i] + " ");
        }
        String originalCommand = sb.toString();
        Logger.log("Original command is: " + originalCommand);

        // FROM SECTION
        String fromSection = this.extractMiddleSection(originalCommand, "SELECT ", " FROM");
        Logger.log("From section: " + fromSection);   

        String fromTableName = this.parseFrom(fromSection);
    
        return true;
    }

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException{
        if (command.length < 4){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        parseSelect(command);
        return true; 

        // Check keywords (case-insensitive)
        // if (!command[0].equalsIgnoreCase("select") || 
        //     !command[1].equals("*") || 
        //     !command[2].equalsIgnoreCase("from")){
        //     throw new SQLSyntaxErrorException(
        //             "Invalid select structure: SELECT * FROM <table>;"
        //     );
        // }

        // // Table name - preserve case as provided
        // String tableName = command[3].toLowerCase();

        // // Verify that table exists
        // Catalog cat = Catalog.getInstance();
        // if (!cat.tableExists(tableName)){
        //     throw new SQLSyntaxErrorException(
        //             "Table does not exist: " + tableName
        //     );
        // }

        // // Run the actual select command
        // Logger.log("Running SELECT * FROM " + tableName);
        // StorageManager store = StorageManager.getStorageManager();
        // try {
        //     Page currentPage = store.selectFirstPage(tableName);
            
        //     if (currentPage == null) {
        //         // Empty table
        //         Logger.log("Table " + tableName + " is empty");
        //         return true;
        //     }
            
        //     // Get table schema for column names
        //     TableSchema schema = cat.getTable(tableName);
        //     List<Attribute> attributes = schema.getAttributes();
            
        //     // Collect all rows first to calculate column widths
        //     List<List<Object>> allRows = new ArrayList<>();
        //     while (true){
        //         for (int i = 0; i < currentPage.getNumRows(); i++){
        //             allRows.add(currentPage.getRecord(i));
        //         }
        //         if (currentPage.getNextPage() == -1){
        //             break;
        //         }
        //         currentPage = store.select(currentPage.getNextPage(), tableName);
        //     }
            
        //     // Calculate column widths
        //     int[] columnWidths = new int[attributes.size()];
        //     for (int i = 0; i < attributes.size(); i++) {
        //         // Start with header name length
        //         columnWidths[i] = attributes.get(i).getName().length();
                
        //         // Check all data values
        //         for (List<Object> row : allRows) {
        //             String value = formatValue(row.get(i));
        //             columnWidths[i] = Math.max(columnWidths[i], value.length());
        //         }
                
        //         // Minimum width of 4 for readability
        //         columnWidths[i] = Math.max(columnWidths[i], 4);
        //     }
            
        //     // Print header row
        //     for (int i = 0; i < attributes.size(); i++) {
        //         System.out.print("|");
        //         System.out.print(String.format(" %" + columnWidths[i] + "s ", 
        //             attributes.get(i).getName()));
        //     }
        //     System.out.println("|");
            
        //     // Print separator line
        //     for (int i = 0; i < attributes.size(); i++) {
        //         System.out.print("-");
        //         for (int j = 0; j < columnWidths[i] + 2; j++) {
        //             System.out.print("-");
        //         }
        //     }
        //     System.out.println("-");
            
        //     // Print data rows
        //     for (List<Object> row : allRows) {
        //         for (int i = 0; i < row.size(); i++) {
        //             System.out.print("|");
        //             String value = formatValue(row.get(i));
        //             System.out.print(String.format(" %" + columnWidths[i] + "s ", value));
        //         }
        //         System.out.println("|");
        //     }

        // } catch (Exception e) {
        //     throw new SQLSyntaxErrorException("Error reading from table: " + e.getMessage());
        // }

        //return true;
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
