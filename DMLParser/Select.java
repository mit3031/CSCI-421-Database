package DMLParser;

import AttributeInfo.*;
import Common.Command;
import Catalog.Catalog;
import Catalog.TableSchema;
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

    /**
     * Constant string used to indicate that this table is not new and thus should not be deleted
     * For example, if I run SELECT * FROM t1; theres no projection and no cartesian product is made, thus neither will result in an 
     * extra table that has to be deleted. To avoid any chance of collisions in strings, a random one was made. 
     */
    private static final String NONEWTABLE = "osnoiqnwdoiwqndoqiuiubibiubnd"; 
        
    /**
     * Extracts everything between "start ... end" of a string
     * @param str the string being extracted from
     * @param start the element we begin extracting from
     * @param end the end element (if blank, defaults to end of the string)
     * @param trimExtraction boolean indicating whether to trim the string or not
     * @return the extracted string (the ... in start...end)
     */
    private String extractMiddleSection(String str, String start, String end, boolean trimExtraction){
        // 1. Find the index of the start string and add its length
        int startIndex = str.indexOf(start) + start.length();
        
        // 2. Find the index of the end string
        int endIndex = str.length() - 1;
        if (!end.equals("")){
            endIndex = str.indexOf(end);
        }

        if (trimExtraction){
            return str.substring(startIndex, endIndex).trim();
        }

        return str.substring(startIndex, endIndex);
    }

    /**
     * Extracts everything between "start ... end" of a string
     * @param str the string being extracted from
     * @param start the element we begin extracting from
     * @param end the end element (if blank, defaults to end of the string)
     * @return the extracted string (the ... in start...end)
     */
    private String extractMiddleSection(String str, String start, String end){
        return this.extractMiddleSection(str, start, end, false); 
    }

    /**
     * Checks if a string is present within some string
     * @param str the original string
     * @param strCheck the string being checked for
     * @return true if the string is present, false otherwise
     */
    private boolean stringExists(String str, String strCheck){
        return str.indexOf(strCheck) != -1; 
    }
    

    /**
     * Helper class to track parsing results including table names and 
     * whether they represent temporary resources that need cleanup.
     */
    private static class ParseResult {
        public final String tableName;
        public final boolean isTemporary;

        public ParseResult(String tableName, boolean isTemporary) {
            this.tableName = tableName;
            this.isTemporary = isTemporary;
        }
    }

    // this is the only one that can return the original table (never use NONEWTABLE)
    // if the user does something like SELECT * FROM t1; then no new table is generated from the 
    // FROM clause. 
    // If a new table is generated return the new table name, true
    // if no new table is generated, return the old table name, false
    // this way future parts can get the table name but it won't be deleted by accident. 
    private ParseResult fromParse(String fromSection){
        return new ParseResult("New table???", true); 
    }

    // in theory this should always return a new table 
    private ParseResult whereParse(String whereSection, String tempTableName){
        return new ParseResult(Select.NONEWTABLE, false);
    }

    /**
     * Resolves the attribute name for the order by clause, handling dot notation and ambiguity
     * @param orderSection The raw string of the attribute to sort by
     * @param tableName The name of the table to check against
     * @return The fully resolved attribute name as it appears in the schema
     */
    private String resolveAttribute(String orderSection, String tableName) throws SQLSyntaxErrorException {
        // Clean the input
        String targetAttr = orderSection.trim().toLowerCase();
        if (targetAttr.endsWith(";")) {
            targetAttr = targetAttr.substring(0, targetAttr.length() - 1).trim();
        }

        // get schema from catalog
        Catalog catalog = Catalog.getInstance();
        TableSchema schema = catalog.getTable(tableName);
        if (schema == null) {
            throw new SQLSyntaxErrorException("Table does not exist: " + tableName);
        }

        List<Attribute> attributes = schema.getAttributes();

        // Handles the dot notation
        if (targetAttr.contains(".")) {
            for (Attribute attr : attributes) {
                if (attr.getName().equals(targetAttr)) {
                    return attr.getName();
                }
            }
            throw new SQLSyntaxErrorException("Column not found: " + targetAttr);
        }

        // Handles unqualified notationn, check for ambiguity
        else {
            int matchCount = 0;
            String resolvedName = null;

            for (Attribute attr : attributes) {
                String schemaAttrName = attr.getName();

                // match exact name or suffix
                if (schemaAttrName.equals(targetAttr) || schemaAttrName.endsWith("." + targetAttr)) {
                    matchCount++;
                    resolvedName = schemaAttrName;
                }
            }

            if (matchCount == 0) {
                throw new SQLSyntaxErrorException("Column not found: " + targetAttr);
            } else if (matchCount > 1) {
                throw new SQLSyntaxErrorException("Ambiguous column name: " + targetAttr);
            }

            return resolvedName; // 1 match found exactly
        }
    }

    /**
     * Generates a new TableSchema for the temporary sorting table
     * The sorting attribute is promoted to the primary key, and all other primary keys are demoted
     * @param originalTableName The name of the table to clone
     * @param sortAttribute The resolved name of the attribute to sort by
     * @return A new TableSchema representing the temporary table
     */
    private TableSchema createTempTableSchema(String originalTableName, String sortAttribute) {
        // generates a unique temp table name to avoid collisions
        String uniqueId = java.util.UUID.randomUUID().toString().replace("-", "");
        String newTableName = "$temp_order_" + uniqueId;

        // getting schema
        Catalog catalog = Catalog.getInstance();
        TableSchema originalSchema = catalog.getTable(originalTableName);
        List<Attribute> originalAttributes = originalSchema.getAttributes();
        List<Attribute> newAttributes = new ArrayList<>();

        // just cloning and modifying attributes
        for (Attribute attr : originalAttributes) {
            AttributeDefinition oldDef = attr.getDefinition();
            boolean isSortColumn = attr.getName().equals(sortAttribute);

            // The sorting column becomes the PK and cannot be null
            // All other columns are stripped of PK status but retain their previous nullability
            boolean newIsPrimary = isSortColumn;
            boolean newPossibleNull = isSortColumn ? false : oldDef.getIsPossibleNull();

            AttributeDefinition newDef = null;

            // Instantiate the correct concrete class based on the enum type
            switch (oldDef.getType()) {
                case INTEGER:
                    // IntegerDefinition is the only one that requires the type enum passed in
                    newDef = new IntegerDefinition(oldDef.getType(), newIsPrimary, newPossibleNull);
                    break;
                case DOUBLE:
                    newDef = new DoubleDefinition(newIsPrimary, newPossibleNull);
                    break;
                case BOOLEAN:
                    newDef = new BooleanDefinition(newIsPrimary, newPossibleNull);
                    break;
                case CHAR:
                    newDef = new CharDefinition(newIsPrimary, newPossibleNull, oldDef.getMaxLength());
                    break;
                case VARCHAR:
                    newDef = new VarCharDefinition(newIsPrimary, newPossibleNull, oldDef.getMaxLength());
                    break;
            }

            // Creates the new attribute and add it to the list
            Attribute newAttr = new Attribute(attr.getName(), newDef, attr.getDefaultValue());
            newAttributes.add(newAttr);
        }

        // Return the new schema
        return new TableSchema(newTableName, newAttributes);
    }

    /**
     * Reads all records from the source table and inserts them into the destination table
     * insert handles sorting since the sort column is the PK
     * @param sourceTableName The name of the original table
     * @param destTableName The name of the new temporary sorting table
     */
    private void migrateData(String sourceTableName, String destTableName) throws Exception {
        StorageManager sm = StorageManager.getStorageManager();
        Catalog catalog = Catalog.getInstance();

        // gets the first page of the source table
        Page currentPage = sm.selectFirstPage(sourceTableName);
        if (currentPage == null) {
            return; // Source table is empty, nothing to migrate
        }

        // get the starting address for the destination table
        int destAddress = catalog.getAddressOfPage(destTableName);

        // Loop through all pages of the source table
        while (currentPage != null) {
            List<List<Object>> batch = new ArrayList<>();
            int numRows = currentPage.getNumRows();

            // extract all records from the current page
            for (int i = 0; i < numRows; i++) {
                // create a new arrayList to prevent memory reference issues
                batch.add(new ArrayList<>(currentPage.getRecord(i)));
            }

            //insert the batch into the destination table
            if (!batch.isEmpty()) {
                destAddress = sm.insert(destTableName, batch, destAddress);
            }

            // Move to the next page if it exists
            if (currentPage.getNextPage() != -1) {
                currentPage = sm.select(currentPage.getNextPage(), sourceTableName);
            } else {
                break;
            }
        }
    }

    /**
     * Executes the order by clause by generating a temporary sorted table
     * @param orderSection The raw string containing the attribute to sort by
     * @param tempTableName The table currently being operated on
     * @return A ParseResult containing the new sorted table's name and a flag for cleanup
     */
    private ParseResult orderByParse(String orderSection, String tempTableName) throws SQLSyntaxErrorException {
        try {
            // Resolves the attribute name (handles dot notation and ambiguity)
            String resolvedAttribute = resolveAttribute(orderSection, tempTableName);

            // Generate the new schema with the sorting attribute as the primary Key
            TableSchema newSchema = createTempTableSchema(tempTableName, resolvedAttribute);
            String newTableName = newSchema.getTableName();

            // create the table in the storage manager
            StorageManager.getStorageManager().CreateTable(newSchema);

            //Migrate the data
            migrateData(tempTableName, newTableName);

            // Return the new temporary table to the pipeline
            return new ParseResult(newTableName, true);

        } catch (SQLSyntaxErrorException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error during ORDER BY execution: " + e.getMessage());
        }
    }

    public boolean parseSelect(String[] command) throws SQLSyntaxErrorException{
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < command.length; i++) {
            sb.append(command[i] + " ");
        }
        String originalCommand = sb.toString();
        Logger.log("Original command is: " + originalCommand);

        // Results from each section
        ParseResult fromResult = null;
        ParseResult whereResult = null;
        ParseResult orderByResult = null;

        // FROM SECTION
        String extractedFrom = "";
        if (this.stringExists(originalCommand, "WHERE")){
            extractedFrom = extractMiddleSection(originalCommand, "FROM", "WHERE", true);
        }else if (this.stringExists(originalCommand, "ORDERING BY")){
            extractedFrom = extractMiddleSection(originalCommand, "FROM", "ORDERING BY", true);
        }else{
            extractedFrom = extractMiddleSection(originalCommand, "FROM", "", true);
        }

        Logger.log("Running the from section: " + extractedFrom);
        fromResult = this.fromParse(extractedFrom);
        // every command must have a FROM clause
        String currentWorkingTable = fromResult.tableName;
        Logger.log("Working table from FROM: " + currentWorkingTable);

        // WHERE SECTION (if WHERE exists)
        if (this.stringExists(originalCommand, "WHERE")){
            Logger.log("WHERE clause detected, running the parse logic");
            
            String extractedWhere = "";
            if (this.stringExists(originalCommand, "ORDERING BY")){
                extractedWhere = this.extractMiddleSection(originalCommand, "WHERE", "ORDERING BY", true);
            }else{
                extractedWhere = this.extractMiddleSection(originalCommand, "WHERE", "", true);
            }

            Logger.log("Running where parse on: " + extractedWhere);
            whereResult = this.whereParse(extractedWhere, currentWorkingTable);
            
            if (!whereResult.tableName.equals(Select.NONEWTABLE)) {
                currentWorkingTable = whereResult.tableName;
            }
            Logger.log("Working table after WHERE: " + currentWorkingTable);
        }

        // ORDERING BY SECTION (if ordering by exists)
        if (this.stringExists(originalCommand, "ORDERING BY")){
            Logger.log("Ordering clause detected");
            String extractedOrderBy = this.extractMiddleSection(originalCommand, "ORDERING BY", "", true); 

            Logger.log("running ordering parse on: " + extractedOrderBy); 
            orderByResult = this.orderByParse(extractedOrderBy, currentWorkingTable);

            if (!orderByResult.tableName.equals(Select.NONEWTABLE)) {
                currentWorkingTable = orderByResult.tableName;
            }
            Logger.log("Working table after ORDERING: " + currentWorkingTable);
        }


        // SELECT section
        String extractedSelect = this.extractMiddleSection(originalCommand, "SELECT", "FROM", true);
        Logger.log("SELECT section: " + extractedSelect);

        if (extractedSelect.equals("*")) {
            Logger.log("Executing SELECT * on " + currentWorkingTable);
            // TODO: Implementation
        } else {
            Logger.log("Executing projection " + extractedSelect + " on " + currentWorkingTable);
            // TODO: Implementation
        }

        // CLEANUP SECTION
        // We delete if isTemporary is true and it hasn't been passed forward to the next stage
        // Note: In a real implementation, even if passed forward, you might delete the 'parent' temp table
        // once the 'child' temp table is fully materialized.

        if (orderByResult != null && orderByResult.isTemporary) {
            Logger.log("Deleting temp order table: " + orderByResult.tableName);
            Catalog.getInstance().dropTable(orderByResult.tableName);
        }
        
        if (whereResult != null && whereResult.isTemporary) {
            // Only delete if it's not the same as the order table (which might have just been deleted)
            if (orderByResult == null || !orderByResult.tableName.equals(whereResult.tableName)) {
                Logger.log("Deleting temp where table: " + whereResult.tableName);
                Catalog.getInstance().dropTable(whereResult.tableName);
            }
        }

        if (fromResult != null && fromResult.isTemporary) {
            // Only delete if it's not the same as where or order tables
            boolean matchesWhere = (whereResult != null && whereResult.tableName.equals(fromResult.tableName));
            boolean matchesOrder = (orderByResult != null && orderByResult.tableName.equals(fromResult.tableName));
            
            if (!matchesWhere && !matchesOrder) {
                Logger.log("Deleting temp from table: " + fromResult.tableName);
                Catalog.getInstance().dropTable(fromResult.tableName);
            }
        }

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
