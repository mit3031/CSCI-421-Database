package DMLParser;

import AttributeInfo.Attribute;
import AttributeInfo.AttributeDefinition;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

public class Update implements Command {

    /**
     * Extracts everything in the middle of the string
     */
    private String extractMiddleSection(String str, String start, String end, boolean trimExtraction){
        String upperStr = str.toUpperCase();
        // padding with spaces to avoid matching parts of table names
        String upperStart = " " + start.toUpperCase() + " ";
        String upperEnd = " " + end.toUpperCase() + " ";

        // Adds a leading space to the original string so the first keyword is found
        upperStr = " " + upperStr;
        str = " " + str;

        int startIndex = upperStr.indexOf(upperStart) + upperStart.length() - 1;

        int endIndex = str.length() - 1;
        if (!end.equals("")){
            endIndex = upperStr.indexOf(upperEnd);
        }

        if (trimExtraction){
            return str.substring(startIndex, endIndex).trim();
        }
        return str.substring(startIndex, endIndex);
    }

    private boolean stringExists(String str, String strCheck){
        return (" " + str.toUpperCase() + " ").contains(" " + strCheck.toUpperCase() + " ");
    }

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        //this reconstructs the command string
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < command.length; i++) {
            sb.append(command[i]).append(" ");
        }
        String originalCommand = sb.toString().trim();
        if (originalCommand.endsWith(";")) {
            originalCommand = originalCommand.substring(0, originalCommand.length() - 1).trim();
        }

        Logger.log("Original UPDATE command is: " + originalCommand);

        // validating the syntax
        if (!stringExists(originalCommand, "UPDATE") || !stringExists(originalCommand, "SET")) {
            throw new SQLSyntaxErrorException("Invalid syntax. Expected: UPDATE <table> SET <attr> = <value> [WHERE <condition>];");
        }

        // this takes all the components
        String tableName = extractMiddleSection(originalCommand, "UPDATE", "SET", true);
        String setClause = "";
        String whereClause = "";
        boolean hasWhere = stringExists(originalCommand, "WHERE");

        if (hasWhere) {
            setClause = extractMiddleSection(originalCommand, "SET", "WHERE", true);
            whereClause = extractMiddleSection(originalCommand, "WHERE", "", true);
        } else {
            setClause = extractMiddleSection(originalCommand, "SET", "", true);
        }

        // this parses the set clause
        if (!setClause.contains("=")) {
            throw new SQLSyntaxErrorException("Invalid SET clause. Expected format: <attribute> = <value>");
        }
        String[] setParts = setClause.split("=", 2);
        String targetAttr = setParts[0].trim();
        String rawNewValue = setParts[1].trim();

        // Verifies if table and attribute exist
        Catalog catalog = Catalog.getInstance();
        if (!catalog.tableExists(tableName)) {
            throw new SQLSyntaxErrorException("Table does not exist: " + tableName);
        }

        TableSchema schema = catalog.getTable(tableName);
        List<Attribute> attributes = schema.getAttributes();

        int targetColIndex = -1;
        Attribute targetAttribute = null;
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getName().equalsIgnoreCase(targetAttr)) {
                targetColIndex = i;
                targetAttribute = attributes.get(i);
                break;
            }
        }

        if (targetColIndex == -1) {
            throw new SQLSyntaxErrorException("Column '" + targetAttr + "' not found in table '" + tableName + "'");
        }

        // Type check and convert the new value
        Object convertedValue = convertAndValidate(rawNewValue, targetAttribute.getDefinition(), targetAttr);

        // *** Executes the update ***
        StorageManager store = StorageManager.getStorageManager();
        int recordsUpdated = 0;
        String tempTableName = "temp_update_" + System.currentTimeMillis();

        try {
            Page currentPage = store.selectFirstPage(tableName);

            if (currentPage == null) {
                Logger.log("Table " + tableName + " is empty. Nothing to update.");
                System.out.println("0 rows updated.");
                return true;
            }

            // Creates temporary table to hold the updated data
            TableSchema tempSchema = new TableSchema(tempTableName, new ArrayList<>(attributes));
            store.CreateTable(tempSchema);

            while (currentPage != null) {
                for (int i = 0; i < currentPage.getNumRows(); i++) {
                    List<Object> row = currentPage.getRecord(i);
                    boolean shouldUpdate = false;

                    if (!hasWhere) {
                        shouldUpdate = true; // update all rows
                    } else {

                        // TODO: Where stuff in here

                        Logger.log("Evaluating WHERE condition: " + whereClause + " for row: " + row);
                        shouldUpdate = true; // stubbed to true for testing
                    }

                    // [NEW] Clone the row to prepare for insertion into the temp table
                    List<Object> newRow = new ArrayList<>(row);

                    if (shouldUpdate) {
                        // modify the row data in memory (applies to the clone)
                        newRow.set(targetColIndex, convertedValue);
                        recordsUpdated++;
                    }

                    // Insert into the temporary table
                    store.insertSingleRow(tempTableName, newRow, 0);
                }

                if (currentPage.getNextPage() == -1) break;
                currentPage = store.select(currentPage.getNextPage(), tableName);
            }

            // Swaps tables, delete the old one and rename the temp one to the original name
            store.DropTable(schema);
            catalog.renameTable(tempTableName, tableName);

            System.out.println(recordsUpdated + " row(s) updated successfully.");

        } catch (Exception e) {
            // Cleans up the temp table if something fails
            try {
                if (catalog.tableExists(tempTableName)) {
                    store.DropTable(catalog.getTable(tempTableName));
                }
            } catch (Exception cleanupException) {
                Logger.log("Failed to clean up temp table: " + cleanupException.getMessage());
            }
            throw new SQLSyntaxErrorException("Error during UPDATE execution: " + e.getMessage());
        }

        return true;
    }

    /**
     * Converts a literal string value to a Java Object and validates it against the schema
     */
    private Object convertAndValidate(String value, AttributeDefinition def, String attrName) throws SQLSyntaxErrorException {
        // Handles null
        if (value.equalsIgnoreCase("NULL")) {
            if (!def.getIsPossibleNull()) {
                throw new SQLSyntaxErrorException("Attribute '" + attrName + "' cannot be NULL");
            }
            return null;
        }

        // Clean quotes for strings
        boolean isString = value.startsWith("\"") && value.endsWith("\"") || value.startsWith("'") && value.endsWith("'");
        String cleanValue = isString ? value.substring(1, value.length() - 1) : value;


        if (!def.isValid(value)) {
            // throw new SQLSyntaxErrorException("Invalid value '" + value + "' for attribute '" + attrName + "' of type " + def.getType());
        }

        try {
            switch (def.getType()) {
                case INTEGER:
                    return Integer.parseInt(cleanValue);
                case DOUBLE:
                    return Double.parseDouble(cleanValue);
                case BOOLEAN:
                    if (cleanValue.equalsIgnoreCase("true") || cleanValue.equals("1")) return true;
                    if (cleanValue.equalsIgnoreCase("false") || cleanValue.equals("0")) return false;
                    throw new SQLSyntaxErrorException("Invalid boolean value '" + value + "'");
                case CHAR:
                case VARCHAR:
                    // max length constraint here
                    if (cleanValue.length() > def.getMaxLength()) {
                        throw new SQLSyntaxErrorException("Value too long for " + def.getType() + "(" + def.getMaxLength() + ")");
                    }
                    return cleanValue;
                default:
                    throw new SQLSyntaxErrorException("Unsupported type " + def.getType());
            }
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException("Cannot convert '" + value + "' to " + def.getType());
        }
    }
}