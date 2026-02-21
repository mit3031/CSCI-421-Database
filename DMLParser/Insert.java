package DMLParser;

import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Page;
import StorageManager.StorageManager;
import AttributeInfo.Attribute;
import AttributeInfo.AttributeDefinition;
import AttributeInfo.AttributeTypeEnum;

import java.sql.Array;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

public class Insert implements Command {

    // INSERT <tableName> VALUES ( <row1> ), ( <row2> ), ... ;
    // Each row contains comma-separated values
    // Example: INSERT Student VALUES (1, "John Doe", 3.5), (2, "Jane Smith", 3.8);

    private final String VALID_SYNTAX = "INSERT <table> VALUES ( <val1>, <val2>, ... ), ( ... );";

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {

        // check if there is at least 5 elements: INSERT <table> VALUES <values_clause>
        if (command.length < 4){
            throw new SQLSyntaxErrorException("Invalid Syntax: " + this.VALID_SYNTAX);
        }

        // check if INSERT <table> and VALUES are accounted for
        if (!command[0].equalsIgnoreCase("INSERT") || 
            !command[2].equalsIgnoreCase("VALUES")){
            throw new SQLSyntaxErrorException("Invalid Syntax: " + this.VALID_SYNTAX);
        }

        // Get table name (preserve case from user, but convert to lowercase for lookups)
        String tableName = command[1].toLowerCase();
        
        // Check if table exists
        Catalog catalog = Catalog.getInstance();
        if (!catalog.tableExists(tableName)){
            throw new SQLSyntaxErrorException("Table does not exist: " + tableName);
        }

        // Get table schema and attributes
        TableSchema table = catalog.getTable(tableName);
        List<Attribute> attributes = table.getAttributes();
        int numAttributes = attributes.size();

        // Parse the VALUES clause - it's in command[4] as a single string
        String valuesString = command[3].trim();

        // Parse rows - split by "),(" pattern
        List<List<String>> parsedRows = parseRows(valuesString);

        // Validate each row has correct number of attributes
        for (int i = 0; i < parsedRows.size(); i++) {
            List<String> row = parsedRows.get(i);
            if (row.size() != numAttributes) {
                throw new SQLSyntaxErrorException(
                    "Row " + (i + 1) + " has " + row.size() + " values, expected " + numAttributes
                );
            }
        }

        // Type check and convert each row
        List<List<Object>> typedRows = new ArrayList<>();
        for (int i = 0; i < parsedRows.size(); i++) {
            List<String> row = parsedRows.get(i);
            List<Object> typedRow = new ArrayList<>();
            
            for (int j = 0; j < row.size(); j++) {
                String value = row.get(j);
                Attribute attr = attributes.get(j);
                AttributeDefinition def = attr.getDefinition();
                
                // Type check and convert
                Object convertedValue = convertAndValidate(value, def, attr.getName(), i + 1);
                typedRow.add(convertedValue);
            }
            
            typedRows.add(typedRow);
        }

        // Insert rows one by one and check PK per row
        try {
            StorageManager store = StorageManager.getStorageManager();
            Set<String> pkValuesInBatch = new HashSet<>(); // keep batch duplicates

            for (int i = 0; i < typedRows.size(); i++) {
                List<Object> row = typedRows.get(i);

                // revised PK check checks this row against table and batch
                checkPrimaryKeyViolations(tableName, attributes, row, pkValuesInBatch, i + 1);

                // Insert the row
                store.insertSingleRow(tableName, row);
            }

            System.out.println("Successfully inserted " + typedRows.size() + " row(s) into " + tableName);

        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error inserting data: " + e.getMessage());
        }

        return true;
    }
    /**
     * Parses the VALUES clause to extract individual rows
     * JottQL syntax: VALUES ( val1, val2, val3 )
     * - Commas separate ROWS (tuples)
     * - Spaces separate COLUMNS within a row
     */
    private List<List<String>> parseRows(String valuesString) throws SQLSyntaxErrorException {
        List<List<String>> rows = new ArrayList<>();
        
        // Remove outer whitespace and parentheses
        valuesString = valuesString.trim();
        if (valuesString.startsWith("(") && valuesString.endsWith(")")) {
            valuesString = valuesString.substring(1, valuesString.length() - 1);
        }
        
        // Split by commas (but not inside quotes) to get individual rows
        List<String> rowStrings = splitByComma(valuesString);
        
        for (String rowString : rowStrings) {
            rows.add(parseRowValues(rowString.trim()));
        }
        
        if (rows.isEmpty()) {
            throw new SQLSyntaxErrorException("No rows found in VALUES clause");
        }
        
        return rows;
    }
    
    /**
     * Splits a string by commas, but not commas inside quotes
     */
    private List<String> splitByComma(String input) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';
        
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            
            if (!inQuotes && (c == '"' || c == '\'')) {
                inQuotes = true;
                quoteChar = c;
                current.append(c);
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
                current.append(c);
            } else if (!inQuotes && c == ',') {
                // Comma outside quotes - split here
                if (current.length() > 0) {
                    result.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        // Add last value
        if (current.length() > 0) {
            result.add(current.toString());
        }
        
        return result;
    }

    /**
     * Parses individual column values within a row
     * Spaces separate columns: val1 val2 val3
     * Preserves quotes around string values
     */
    private List<String> parseRowValues(String rowString) throws SQLSyntaxErrorException {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';
        
        rowString = rowString.trim();
        
        for (int i = 0; i < rowString.length(); i++) {
            char c = rowString.charAt(i);
            
            if (!inQuotes && (c == '"' || c == '\'')) {
                // Start of quoted string - include the quote
                inQuotes = true;
                quoteChar = c;
                current.append(c);
            } else if (inQuotes && c == quoteChar) {
                // End of quoted string - include the closing quote
                inQuotes = false;
                current.append(c);
            } else if (!inQuotes && Character.isWhitespace(c)) {
                // Space outside quotes - end of current value
                if (current.length() > 0) {
                    values.add(current.toString().trim());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        // Add last value
        if (current.length() > 0) {
            values.add(current.toString().trim());
        }
        
        if (values.isEmpty()) {
            throw new SQLSyntaxErrorException("Empty row in VALUES clause");
        }
        
        return values;
    }

    /**
     * Converts a string value to the appropriate type and validates it
     */
    private Object convertAndValidate(String value, AttributeDefinition def, 
                                     String attrName, int rowNum) throws SQLSyntaxErrorException {
        
        // Handle NULL
        if (value.equalsIgnoreCase("NULL")) {
            if (!def.getIsPossibleNull()) {
                throw new SQLSyntaxErrorException(
                    "Row " + rowNum + ": Attribute '" + attrName + "' cannot be NULL"
                );
            }
            return null;
        }

        // For VARCHAR and CHAR, validate WITH quotes first (as the isValid method expects them)
        boolean isStringType = (def.getType() == AttributeTypeEnum.VARCHAR || 
                               def.getType() == AttributeTypeEnum.CHAR);
        
        if (isStringType) {
            // Validate using the AttributeDefinition's isValid method (expects quotes)
            if (!def.isValid(value)) {
                throw new SQLSyntaxErrorException(
                    "Row " + rowNum + ": Invalid value '" + value + "' for attribute '" + 
                    attrName + "' of type " + def.getType()
                );
            }
            
            // Remove quotes for storage
            if ((value.startsWith("\"") && value.endsWith("\"")) ||
                (value.startsWith("'") && value.endsWith("'"))) {
                value = value.substring(1, value.length() - 1);
            }
            
            return value;
        }

        // For non-string types, remove quotes if present (shouldn't normally have them)
        if ((value.startsWith("\"") && value.endsWith("\"")) ||
            (value.startsWith("'") && value.endsWith("'"))) {
            value = value.substring(1, value.length() - 1);
        }

        // Validate using the AttributeDefinition's isValid method
        if (!def.isValid(value)) {
            throw new SQLSyntaxErrorException(
                "Row " + rowNum + ": Invalid value '" + value + "' for attribute '" + 
                attrName + "' of type " + def.getType()
            );
        }

        // Convert to appropriate type
        try {
            switch (def.getType()) {
                case INTEGER:
                    return Integer.parseInt(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case BOOLEAN:
                    // Accept: true/false, TRUE/FALSE, 1/0
                    if (value.equalsIgnoreCase("true") || value.equals("1")) {
                        return true;
                    } else if (value.equalsIgnoreCase("false") || value.equals("0")) {
                        return false;
                    } else {
                        throw new SQLSyntaxErrorException(
                            "Row " + rowNum + ": Invalid boolean value '" + value + "'"
                        );
                    }
                case CHAR:
                case VARCHAR:
                    return value;
                default:
                    throw new SQLSyntaxErrorException(
                        "Row " + rowNum + ": Unsupported type " + def.getType()
                    );
            }
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(
                "Row " + rowNum + ": Cannot convert '" + value + "' to " + def.getType()
            );
        }
    }

    /**
     * Checks for primary key violations by:
     * 1. Checking for duplicates within the batch being inserted
     * 2. Checking against existing data in the table
     *
     * Handles row by row insertion. The "row" argument is a single row to check,
     * and "pkValuesInBatch" tracks PKs that are already inserted in the batch
     */
    private void checkPrimaryKeyViolations(String tableName, List<Attribute> attributes,
                                           List<Object> row, Set<String> pkValuesInBatch,
                                           int rowNum) throws SQLSyntaxErrorException {

        // Find primary key column(s)
        List<Integer> pkIndices = new ArrayList<>();
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getDefinition().getIsPrimary()) {
                pkIndices.add(i);
            }
        }

        // If no primary key, no need to check
        if (pkIndices.isEmpty()) {
            return;
        }

        // Check for duplicates within the batch
        StringBuilder pkValue = new StringBuilder();
        for (int pkIdx : pkIndices) {
            Object val = row.get(pkIdx);
            if (val == null) {
                throw new SQLSyntaxErrorException(
                        "Row " + rowNum + ": Primary key cannot be NULL"
                );
            }
            pkValue.append(val.toString()).append("|");
        }
        String pkKey = pkValue.toString();
        if (pkValuesInBatch.contains(pkKey)) {
            throw new SQLSyntaxErrorException(
                    "Row " + rowNum + ": Duplicate primary key value in batch"
            );
        }

        // Check against existing data in table
        try {
            StorageManager store = StorageManager.getStorageManager();
            Page currentPage = store.selectFirstPage(tableName);

            while (currentPage != null) {
                for (int i = 0; i < currentPage.getNumRows(); i++) {
                    List<Object> existingRow = currentPage.getRecord(i);
                    StringBuilder existingPK = new StringBuilder();
                    for (int pkIdx : pkIndices) {
                        Object val = existingRow.get(pkIdx);
                        existingPK.append(val != null ? val.toString() : "NULL").append("|");
                    }
                    if (existingPK.toString().equals(pkKey)) {
                        throw new SQLSyntaxErrorException(
                                "Row " + rowNum + ": Primary key violation, value already exists in table"
                        );
                    }
                }

                if (currentPage.getNextPage() == -1) break;
                currentPage = store.select(currentPage.getNextPage(), tableName);
            }

        } catch (Exception e) {
            if (e instanceof SQLSyntaxErrorException) {
                throw (SQLSyntaxErrorException) e;
            }
            throw new SQLSyntaxErrorException("Error checking primary key constraints: " + e.getMessage());
        }

        // Add pk to batch set if inserted
        pkValuesInBatch.add(pkKey);
    }
}