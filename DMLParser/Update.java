package DMLParser;

import AttributeInfo.Attribute;
import AttributeInfo.AttributeDefinition;
import AttributeInfo.AttributeTypeEnum;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;
import Common.Page;
import Common.Where.BuildTree;
import Common.Where.IWhereOp;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        // *** Executes the update ***
        StorageManager store = StorageManager.getStorageManager();
        int recordsUpdated = 0;
        String tempTableName = "temp_update_" + System.currentTimeMillis();

        try {
            Page currentPage = store.selectFirstPage(tableName);

            if (currentPage == null) {
                Logger.log("Table " + tableName + " is empty. Nothing to update");
                System.out.println("0 rows updated.");
                return true;
            }

            // Creates temporary table to hold the updated data
            TableSchema tempSchema = new TableSchema(tempTableName, new ArrayList<>(attributes));
            store.CreateTable(tempSchema);

            // Builds the where tree if a where clause exists
            IWhereOp whereTree = null;
            if (hasWhere) {
                whereTree = BuildTree.buildTree(whereClause, schema);
                if (whereTree == null) {
                    throw new SQLSyntaxErrorException("Invalid WHERE clause syntax or types");
                }
            }
            // keep track of primary keys inserted into the temp table to prevent duplicates
            Set<String> tempTablePKs = new HashSet<>();

            while (currentPage != null) {
                for (int i = 0; i < currentPage.getNumRows(); i++) {
                    List<Object> row = currentPage.getRecord(i);
                    boolean shouldUpdate = false;

                    if (!hasWhere) {
                        shouldUpdate = true; // update all rows
                    } else {
                        // Where evaluation here
                        shouldUpdate = whereTree.evaluate(new ArrayList<>(row), schema);
                    }

                    // clone the row to prepare for insertion into the temp table
                    List<Object> newRow = new ArrayList<>(row);

                    if (shouldUpdate) {
                        // evaluate the value for this row (handles math and attribute references)
                        Object computedValue = evaluateExpression(rawNewValue, row, schema, targetAttribute);

                        // Checks for nutnull constraint manually before insertion
                        if (computedValue == null && !targetAttribute.getDefinition().getIsPossibleNull()) {
                            throw new SQLSyntaxErrorException("Attribute '" + targetAttr + "' cannot be NULL");
                        }

                        // modify the row data
                        newRow.set(targetColIndex, computedValue);
                        recordsUpdated++;
                    }

                    // extract the PK from the newRow based on schema for violations
                    StringBuilder pkBuilder = new StringBuilder();
                    for (int j = 0; j < attributes.size(); j++) {
                        if (attributes.get(j).getDefinition().getIsPrimary()) {
                            pkBuilder.append(newRow.get(j)).append("|");
                        }
                    }
                    String pkKey = pkBuilder.toString();

                    // If this update causes a duplicate primary key, throw an error
                    if (!pkKey.isEmpty()) {
                        if (tempTablePKs.contains(pkKey)) {
                            throw new SQLSyntaxErrorException("UPDATE would result in duplicate Primary Key: " + pkKey.replace("|", ""));
                        }
                        tempTablePKs.add(pkKey);
                    }

                    // Insert into the temporary table instead of modifying the page directly
                    store.insertSingleRow(tempTableName, newRow, 0);
                }

                if (currentPage.getNextPage() == -1) {
                    break;
                }
                currentPage = store.select(currentPage.getNextPage(), tableName);
            }

            // Swaps tables, delete the old one and rename the temp one to the original name
            store.DropTable(schema);
            catalog.renameTable(tempTableName, tableName);

            System.out.println(recordsUpdated + " rows updated successfully.");

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
     * Checks if the right hand side is a math expression, attribute, or literal
     */
    private Object evaluateExpression(String expr, List<Object> row, TableSchema schema, Attribute targetAttr) throws SQLSyntaxErrorException {
        AttributeTypeEnum type = targetAttr.getDefinition().getType();
        boolean isNumeric = (type == AttributeTypeEnum.INTEGER || type == AttributeTypeEnum.DOUBLE);

        // Check for math operators only if the target is numeric
        if (isNumeric) {
            String[] operators = {"+", "-", "*", "/"};
            String foundOp = null;

            for (String op : operators) {
                if (expr.contains(op)) {
                    foundOp = op;
                    break;
                }
            }

            if (foundOp != null) {
                int opIndex = expr.indexOf(foundOp);
                String leftExpr = expr.substring(0, opIndex).trim();
                String rightExpr = expr.substring(opIndex + foundOp.length()).trim();

                Object leftVal = resolveOperand(leftExpr, row, schema, targetAttr);
                Object rightVal = resolveOperand(rightExpr, row, schema, targetAttr);

                return performMath(leftVal, rightVal, foundOp, targetAttr);
            }
        }

        // If no operator found it will either be a single attribute or a literal
        return resolveOperand(expr, row, schema, targetAttr);
    }

    /**
     * Resolves a single term to either a value from the current row, or parses it as a literal
     */
    private Object resolveOperand(String val, List<Object> row, TableSchema schema, Attribute targetAttr) throws SQLSyntaxErrorException {
        if (val.equalsIgnoreCase("NULL")) {
            return null;
        }

        // checks if the term matches another attributes name in the table
        List<Attribute> attrs = schema.getAttributes();
        for (int i = 0; i < attrs.size(); i++) {
            if (attrs.get(i).getName().equalsIgnoreCase(val)) {
                return row.get(i);
            }
        }

        // If its not an attribute, treat it as a literal value
        return convertAndValidate(val, targetAttr.getDefinition(), targetAttr.getName());
    }

    /**
     * Performs the math operations
     */
    private Object performMath(Object left, Object right, String op, Attribute targetAttr) throws SQLSyntaxErrorException {
        if (left == null || right == null) {
            return null;
        }

        AttributeTypeEnum type = targetAttr.getDefinition().getType();

        if (type == AttributeTypeEnum.INTEGER) {
            Integer l = (Integer) left;
            Integer r = (Integer) right;

            switch(op) {
                case "+":
                    return l + r;
                case "-":
                    return l - r;
                case "*":
                    return l * r;
                case "/":
                    if (r == 0) {
                        throw new SQLSyntaxErrorException("Division by zero");
                    }
                    return l / r;
            }
        } else if (type == AttributeTypeEnum.DOUBLE) {
            // allow adding an integer to a double column
            Double l;
            if (left instanceof Integer) {
                l = ((Integer) left).doubleValue();
            } else {
                l = (Double) left;
            }

            Double r;
            if (right instanceof Integer) {
                r = ((Integer) right).doubleValue();
            } else {
                r = (Double) right;
            }

            switch(op) {
                case "+":
                    return l + r;
                case "-":
                    return l - r;
                case "*":
                    return l * r;
                case "/":
                    if (r == 0.0) {
                        throw new SQLSyntaxErrorException("Division by zero");
                    }
                    return l / r;
            }
        }
        throw new SQLSyntaxErrorException("Math operations only supported on INTEGER and DOUBLE");
    }

    /**
     * Converts a literal string value to a Java Object and validates it against the schema
     */
    private Object convertAndValidate(String value, AttributeDefinition def, String attrName) throws SQLSyntaxErrorException {
        // Clean quotes for strings
        boolean isString = (value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"));

        String cleanValue = value;
        if (isString) {
            cleanValue = value.substring(1, value.length() - 1);
        }

        try {
            switch (def.getType()) {
                case INTEGER:
                    return Integer.parseInt(cleanValue);
                case DOUBLE:
                    return Double.parseDouble(cleanValue);
                case BOOLEAN:
                    if (cleanValue.equalsIgnoreCase("true") || cleanValue.equals("1")) {
                        return true;
                    }
                    if (cleanValue.equalsIgnoreCase("false") || cleanValue.equals("0")) {
                        return false;
                    }
                    throw new SQLSyntaxErrorException("Invalid boolean value '" + value);
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