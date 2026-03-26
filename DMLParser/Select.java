package DMLParser;

import AttributeInfo.*;
import Common.Command;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Logger;
import Common.Page;
import Common.Where.*;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.stream.Collectors;

import static Common.Where.BuildTree.buildTree;

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
    private ParseResult fromParse(String fromSection) throws Exception {
        // When cartesian product is implemented, create temp table with qualified attribute names
        // Example: FROM A, B where A has (x,y) and B has (x,q) -> create temp table with (A.x, A.y, B.x, B.q)
        String TEMP_TABLE_NAME = "$temp";
        //String tableName = fromSection.trim();

        // Check if table exists
        Catalog catalog = Catalog.getInstance();
        StorageManager storageManager = StorageManager.getStorageManager();

        ArrayList<String> tableNames = new ArrayList<>(Arrays.asList(fromSection.split(",")));

        // Check if tables exist and ensure there is no whitespace attached
        for ( int i = 0; i < tableNames.size(); i++ ) {
            String tableName = tableNames.get(i).trim();
            tableNames.set(i, tableName);
            if (!catalog.tableExists(tableName)){
                throw new SQLSyntaxErrorException("Table: " + tableName + " does not exist");
            }
        }

        // Single table so just return original table
        if (tableNames.size() == 1){
            return new ParseResult(tableNames.get(0), false);
        }

        // Multiple tables need to do a cartesian product

        // while (length of table names > 1)
        // get first two tables
        // create a list of combined attributes
            // Rename attriubtes to be tablename.attrname as long as current attrname doesn't contian a '.'
            // Add them into  alist together
        // create temp table
        // for rows in t1
            // for rows in t2
                // combine t1 row and t2 row, add to table
        // remove first element
        // make second element equal to new temp table

        int tableNameCounter = 0;
        while (tableNames.size() > 1) {
            TableSchema table1 = catalog.getTable(tableNames.get(0));
            TableSchema table2 = catalog.getTable(tableNames.get(1));
            List<Attribute> newAttributes = new ArrayList<>();

            //Lambda statement to rename attributes and add them to the new list of attributes
            table1.getAttributes().forEach(attr -> newAttributes.add(Attribute.rename(attr, (attr.getName().contains(".")) ? table1.getTableName()+"."+attr.getName() : attr.getName())));
            table2.getAttributes().forEach(attr -> newAttributes.add(Attribute.rename(attr, (attr.getName().contains(".")) ? table2.getTableName()+"."+attr.getName() : attr.getName())));

            // Create a temporary table, tableNameCounter is used in the case that there are 3 or more tables being
            // combined and there will be an instance where more than one temp table will need to be created
            TableSchema tempTable = new TableSchema(TEMP_TABLE_NAME + tableNameCounter, newAttributes);
            storageManager.CreateTable(tempTable);

            // Get the first page for the two tables
            Page table1Page = storageManager.selectFirstPage(table1.getTableName());

            int address = catalog.getAddressOfPage(tempTable.getTableName());

            // Perform cartesian product and insert them into the temporary table
            while (true){

                for (int i = 0; i < table1Page.getNumRows(); i++){
                    Page table2Page = storageManager.selectFirstPage(table2.getTableName());

                    // for each row in table 1 add every row from table 2
                    while(true) {
                        List<List<Object>> newRows = new ArrayList<>();
                        for (int j = 0; j < table2Page.getNumRows(); j++) {
                            List<Object> newRow = new ArrayList<>();
                            newRow.addAll(table1Page.getRecord(i));
                            newRow.addAll(table2Page.getRecord(j));
                            newRows.add(newRow);
                        }

                        // Insert rows in batches, a batch is one row from table1 combined with all rows from table2 in one page
                        address = storageManager.heapInsert(tempTable.getTableName(), newRows, address);
                        //if there is a new page move to it, otherwise move back to the outside table
                        if (table2Page.getNextPage() != -1) {
                            table2Page = storageManager.select(table2Page.getNextPage(), table2.getTableName());
                        } else {
                            break;
                        }
                    }
                }

                if(table1Page.getNextPage() != -1){
                    table1Page = storageManager.select(table1Page.getNextPage(), table1.getTableName());
                } else {
                    break;
                }

            }
            String usedTable = tableNames.remove(0);
            if(tableNames.get(0).contains("$temp")){
                storageManager.DropTable(catalog.getTable(usedTable));
            }
            tableNames.set(0, tempTable.getTableName());
            tableNameCounter++;

        }


        return new ParseResult(tableNames.get(0), true);
    }

    // in theory this should always return a new table 
    private ParseResult whereParse(String whereSection, String tempTableName) throws Exception {
        Catalog catalog = Catalog.getInstance();
        TableSchema newTable = new TableSchema("$where", catalog.getTable(tempTableName).getAttributes());

        IWhereOp whereTree = buildTree(whereSection, newTable);
        if (whereTree == null) {
            return new ParseResult("error", true);
        }
        StorageManager storageManager = StorageManager.getStorageManager();
        storageManager.CreateTable(newTable);

        //find start of old table
        Page page = storageManager.selectFirstPage(tempTableName);
        int address = catalog.getAddressOfPage("$where");
        int nextPage = -1;
        while (page != null) {
            for (int i = 0; i < page.getNumRows(); i++) {
                if (whereTree.evaluate(page.getRecord(i), newTable)) {
                    address = storageManager.insertSingleRow("$where", page.getRecord(i), address);
                }
            }
            nextPage = page.getNextPage();
                if(nextPage!= -1) {
                    page = storageManager.select(nextPage, tempTableName);
                }
                else{
                    page = null;
                }

        }
        return new ParseResult("$where", true);
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
            String[] parts = targetAttr.split("\\.");
            if (parts.length != 2) {
                throw new SQLSyntaxErrorException("Invalid qualified attribute name: " + targetAttr);
            }

            String reqTable = parts[0];
            String reqAttr = parts[1];

            for (Attribute attr : attributes) {
                String schemaAttrName = attr.getName().toLowerCase();

                //  Match exactly for cartesian products where schema name is actually table.col
                if (schemaAttrName.equals(targetAttr)) {
                    return attr.getName();
                }
                // Match unqualified schema name for single tables where schema is col but user typed table.col
                else if (schemaAttrName.equals(reqAttr) &&
                        (tableName.equalsIgnoreCase(reqTable) || tableName.startsWith("$temp_"))) {
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
                if (schemaAttrName.toLowerCase().equals(targetAttr) || schemaAttrName.toLowerCase().endsWith("." + targetAttr)) {
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
     * @param sortAttribute The name of the column to sort the records by
     */
    private void migrateData(String sourceTableName, String destTableName, String sortAttribute) throws Exception {
        StorageManager sm = StorageManager.getStorageManager();
        Catalog catalog = Catalog.getInstance();

        // Finds the index of the column we want to sort by
        TableSchema schema = catalog.getTable(sourceTableName);
        int sortColIndex = -1;
        for (int i = 0; i < schema.getAttributes().size(); i++) {
            if (schema.getAttributes().get(i).getName().equals(sortAttribute)) {
                sortColIndex = i;
                break;
            }
        }

        // reads all records into memory
        List<List<Object>> allRecords = new ArrayList<>();
        Page currentPage = sm.selectFirstPage(sourceTableName);

        while (currentPage != null) {
            for (int i = 0; i < currentPage.getNumRows(); i++) {
                allRecords.add(new ArrayList<>(currentPage.getRecord(i)));
            }
            if (currentPage.getNextPage() != -1) {
                currentPage = sm.select(currentPage.getNextPage(), sourceTableName);
            } else {
                break;
            }
        }

        if (allRecords.isEmpty()) return;

        // Sorts the records in memory
        final int colIndex = sortColIndex;
        allRecords.sort((row1, row2) -> {
            Object val1 = row1.get(colIndex);
            Object val2 = row2.get(colIndex);

            if (val1 == null && val2 == null) return 0;
            if (val1 == null) return -1; // Nulls come first
            if (val2 == null) return 1;

            return ((Comparable<Object>) val1).compareTo(val2);
        });

        // insert the newly sorted batch into the destination table
        int destAddress = catalog.getAddressOfPage(destTableName);
        sm.insert(destTableName, allRecords, destAddress);
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
            migrateData(tempTableName, newTableName, resolvedAttribute);

            // Return the new temporary table to the pipeline
            return new ParseResult(newTableName, true);

        } catch (SQLSyntaxErrorException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error during ORDER BY execution: " + e.getMessage());
        }
    }
  

    /**
     * Executes a SELECT * query on the specified table
     * @param tableName the name of the table to select from
     * @throws SQLSyntaxErrorException if there's an error reading from the table
     */
    private void executeSelectAll(String tableName) throws SQLSyntaxErrorException {
        Catalog cat = Catalog.getInstance();

        // Verify that table exists
        if (!cat.tableExists(tableName)){
            throw new SQLSyntaxErrorException("Table does not exist: " + tableName);
        }

        StorageManager store = StorageManager.getStorageManager();
        try {
            Page currentPage = store.selectFirstPage(tableName);

            if (currentPage == null) {
                Logger.log("Table " + tableName + " is empty");
                System.out.println("Table " + tableName + " is empty");
                return;
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
                columnWidths[i] = attributes.get(i).getName().length();

                for (List<Object> row : allRows) {
                    String value = formatValue(row.get(i));
                    columnWidths[i] = Math.max(columnWidths[i], value.length());
                }

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
    }

    /**
     * Executes a projection query (SELECT specific columns)
     * @param tableName the name of the table to select from (could be a temp table from cartesian product)
     * @param projection comma-separated list of column names (may include dot notation)
     * @throws SQLSyntaxErrorException if there's an error or column doesn't exist
     */
    private void executeProjection(String tableName, String projection) throws SQLSyntaxErrorException {
        Catalog cat = Catalog.getInstance();

        // Verify that table exists
        if (!cat.tableExists(tableName)){
            throw new SQLSyntaxErrorException("Table does not exist: " + tableName);
        }

        // Parse projection attributes
        List<String> projectionList = Arrays.stream(projection.split(","))
            .map(String::trim)
            .collect(Collectors.toList());

        // Get table schema
        TableSchema schema = cat.getTable(tableName);
        List<Attribute> allAttributes = schema.getAttributes();

        // Find indices of projection attributes
        List<Integer> projectionIndices = new ArrayList<>();
        List<Attribute> projectionAttributes = new ArrayList<>();

        for (String projAttr : projectionList) {
            // Check if this is a qualified name (table.attribute)
            if (projAttr.contains(".")) {
                // Qualified name: table.attribute
                String[] parts = projAttr.split("\\.");
                if (parts.length != 2) {
                    throw new SQLSyntaxErrorException("Invalid qualified attribute name: " + projAttr);
                }

                String requestedTable = parts[0].toLowerCase().trim();
                String requestedAttr = parts[1].toLowerCase().trim();

                // Look for this exact qualified attribute in the schema
                boolean found = false;
                for (int i = 0; i < allAttributes.size(); i++) {
                    String attrName = allAttributes.get(i).getName().toLowerCase();

                    // Check if attribute matches the qualified name
                    // For single tables, attribute should just be the name
                    // For cartesian products, attribute will be "table.attribute"
                    if (attrName.equals(requestedTable + "." + requestedAttr)) {
                        // Exact match with qualified name
                        projectionIndices.add(i);
                        projectionAttributes.add(allAttributes.get(i));
                        found = true;
                        break;
                    } else if (attrName.equals(requestedAttr) && tableName.equalsIgnoreCase(requestedTable)) {
                        // Single table case: user qualified with table name, attribute is unqualified
                        projectionIndices.add(i);
                        projectionAttributes.add(allAttributes.get(i));
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw new SQLSyntaxErrorException("Column not found: " + projAttr);
                }
            } else {
                // Unqualified name: check for ambiguity
                List<Integer> matchingIndices = new ArrayList<>();

                for (int i = 0; i < allAttributes.size(); i++) {
                    String attrName = allAttributes.get(i).getName().toLowerCase();
                    String searchAttr = projAttr.toLowerCase();

                    // Check if attribute name matches (could be qualified or unqualified in schema)
                    if (attrName.equals(searchAttr)) {
                        // Exact match
                        matchingIndices.add(i);
                    } else if (attrName.contains(".") && attrName.endsWith("." + searchAttr)) {
                        // Schema has qualified name (e.g., "a.x"), user searched for unqualified ("x")
                        matchingIndices.add(i);
                    }
                }

                if (matchingIndices.isEmpty()) {
                    throw new SQLSyntaxErrorException("Column not found: " + projAttr);
                } else if (matchingIndices.size() > 1) {
                    throw new SQLSyntaxErrorException("Ambiguous column name: " + projAttr +
                        ". Please qualify with table name (e.g., tablename." + projAttr + ")");
                } else {
                    // Exactly one match - unambiguous
                    projectionIndices.add(matchingIndices.get(0));
                    projectionAttributes.add(allAttributes.get(matchingIndices.get(0)));
                }
            }
        }

        StorageManager store = StorageManager.getStorageManager();
        try {
            Page currentPage = store.selectFirstPage(tableName);

            if (currentPage == null) {
                Logger.log("Table " + tableName + " is empty");
                System.out.println("Table " + tableName + " is empty");
                return;
            }

            // Collect all rows
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

            // Calculate column widths for projection
            int[] columnWidths = new int[projectionAttributes.size()];
            for (int i = 0; i < projectionAttributes.size(); i++) {
                columnWidths[i] = projectionAttributes.get(i).getName().length();

                int colIndex = projectionIndices.get(i);
                for (List<Object> row : allRows) {
                    String value = formatValue(row.get(colIndex));
                    columnWidths[i] = Math.max(columnWidths[i], value.length());
                }

                columnWidths[i] = Math.max(columnWidths[i], 4);
            }

            // Print header row
            for (int i = 0; i < projectionAttributes.size(); i++) {
                System.out.print("|");
                System.out.print(String.format(" %" + columnWidths[i] + "s ",
                    projectionAttributes.get(i).getName()));
            }
            System.out.println("|");

            // Print separator line
            for (int i = 0; i < projectionAttributes.size(); i++) {
                System.out.print("-");
                for (int j = 0; j < columnWidths[i] + 2; j++) {
                    System.out.print("-");
                }
            }
            System.out.println("-");

            // Print data rows (only projection columns)
            for (List<Object> row : allRows) {
                for (int i = 0; i < projectionIndices.size(); i++) {
                    System.out.print("|");
                    int colIndex = projectionIndices.get(i);
                    String value = formatValue(row.get(colIndex));
                    System.out.print(String.format(" %" + columnWidths[i] + "s ", value));
                }
                System.out.println("|");
            }

        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error reading from table: " + e.getMessage());
        }
    }

    public boolean parseSelect(String[] command) throws Exception {
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
        }else if (this.stringExists(originalCommand, "ORDERBY")){
            extractedFrom = extractMiddleSection(originalCommand, "FROM", "ORDERBY", true);
        }else{
            extractedFrom = extractMiddleSection(originalCommand, "FROM", "", true);
        }

        Logger.log("Running the from section: " + extractedFrom);

        // todo needs an overall try catch this was just so it would compile and run
        try{fromResult = this.fromParse(extractedFrom);} catch (Exception e){
            System.out.println("Caught from the from: ");
            e.printStackTrace(System.err);
        }

        // every command must have a FROM clause
        String currentWorkingTable = fromResult.tableName;
        Logger.log("Working table from FROM: " + currentWorkingTable);

        // WHERE SECTION (if WHERE exists)
        if (this.stringExists(originalCommand, "WHERE")){
            Logger.log("WHERE clause detected, running the parse logic");
            
            String extractedWhere = "";
            if (this.stringExists(originalCommand, "ORDERBY")){
                extractedWhere = this.extractMiddleSection(originalCommand, "WHERE", "ORDERBY", true);
            }else{
                extractedWhere = this.extractMiddleSection(originalCommand, "WHERE", "", true);
            }

            Logger.log("Running where parse on: " + extractedWhere);
            whereResult = this.whereParse(extractedWhere, currentWorkingTable);
            if (whereResult.tableName.equals("error")) {
                return false;
            }

            if (!whereResult.tableName.equals(Select.NONEWTABLE)) {
                currentWorkingTable = whereResult.tableName;
            }
            Logger.log("Working table after WHERE: " + currentWorkingTable);
        }

        // ORDERING BY SECTION (if ordering by exists)
        if (this.stringExists(originalCommand, "ORDERBY")){
            Logger.log("Ordering clause detected");
            String extractedOrderBy = this.extractMiddleSection(originalCommand, "ORDERBY", "", true);

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
            this.executeSelectAll(currentWorkingTable);
        } else {
            Logger.log("Executing projection " + extractedSelect + " on " + currentWorkingTable);
            this.executeProjection(currentWorkingTable, extractedSelect);
        }

        // CLEANUP SECTION
        // We delete if isTemporary is true and it hasn't been passed forward to the next stage
        // Note: In a real implementation, even if passed forward, you might delete the 'parent' temp table
        // once the 'child' temp table is fully materialized.

        if (orderByResult != null && orderByResult.isTemporary) {
            Logger.log("Deleting temp order table: " + orderByResult.tableName);
            StorageManager sm = StorageManager.getStorageManager();
            Catalog cat = Catalog.getInstance();
            TableSchema table = cat.getTable(orderByResult.tableName);
            if(table == null){
                Logger.log("Table " + orderByResult.tableName + " not found in catalog");
                return false;
            }
            try {
                sm.DropTable(table);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        
        if (whereResult != null && whereResult.isTemporary) {
            // Only delete if it's not the same as the order table (which might have just been deleted)
            if (orderByResult == null || !orderByResult.tableName.equals(whereResult.tableName)) {
                Logger.log("Deleting temp where table: " + whereResult.tableName);
                StorageManager sm = StorageManager.getStorageManager();
                Catalog cat = Catalog.getInstance();
                TableSchema table = cat.getTable(whereResult.tableName);
                if(table == null){
                    Logger.log("Table " + whereResult.tableName + " not found in catalog");
                    return false;
                }
                try {
                    sm.DropTable(table);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    throw new RuntimeException(e);
                }   
            }
        }

        if (fromResult != null && fromResult.isTemporary) {
            // Only delete if it's not the same as where or order tables
            boolean matchesWhere = (whereResult != null && whereResult.tableName.equals(fromResult.tableName));
            boolean matchesOrder = (orderByResult != null && orderByResult.tableName.equals(fromResult.tableName));
            
            if (!matchesWhere && !matchesOrder) {
                Logger.log("Deleting temp from table: " + fromResult.tableName);
                StorageManager sm = StorageManager.getStorageManager();
                Catalog cat = Catalog.getInstance();
                TableSchema table = cat.getTable(fromResult.tableName);
                if(table == null){
                    Logger.log("Table " + fromResult.tableName + " not found in catalog");
                    return false;
                }
                try {
                    sm.DropTable(table);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        return true;
    }

    @Override
    public boolean run(String[] command) throws Exception {
        if (command.length < 4){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }
        //if false return false?
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
