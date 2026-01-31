package DMLParser;

import Catalog.Catalog;
import Common.Command;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Insert implements Command {

    // INSERT <tableName> VALUES ( <row1>, <row2>, ... );
    // a row is space delimited so for Student(name, id) we might do
    // INSERT Student VALUES ("jacob" 47, "kim" 88);

    private final String VALID_SYNTAX = "INSERT <table> VALUES ( <row1>, <row2>, ..., <rowN> )";

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {

        // check if there is at least 4 elements
        if (command.length < 4){
            throw new SQLSyntaxErrorException("Invalid Syntax: " + this.VALID_SYNTAX);
        }

        // check if INSERT <table> and VALUES are accounted for
        if (!command[0].equals("INSERT") || !command[2].equals("VALUES")){
            throw new SQLSyntaxErrorException("Invalid Syntax: " + this.VALID_SYNTAX);
        }

        // check to see if table exists
        String tableName = command[1];
        // TODO: use the catalog to confirm the table exists

        // pull the attributes of each row
        // TODO: use the catalog to pull the attributes for this table

        // pull the rows using (make sure to filter out each comma and brackets)

        // Combine the remaining parts of the command into a single string to handle internal spacing
        StringBuilder rowDataBuilder = new StringBuilder();
        for (int i = 3; i < command.length; i++) {
            rowDataBuilder.append(command[i]).append(" ");
        }
        String rowData = rowDataBuilder.toString().trim();

        // Remove the outer brackets (first '(' and last ')')
        if (rowData.startsWith("(") && rowData.endsWith(")")) {
            rowData = rowData.substring(1, rowData.length() - 1);
        } else {
            throw new SQLSyntaxErrorException("Missing parentheses in VALUES clause.");
        }

        // Split by "), (" to get individual rows, then clean up remaining commas/whitespace
        String[] rowStrings = rowData.split("\\s*\\)\\s*,\\s*\\(\\s*");
        List<List<String>> parsedRows = new ArrayList<>();

        for (String row : rowStrings) {
            // Split the row by space
            String[] attributes = row.trim().split("\\s+");
            parsedRows.add(Arrays.asList(attributes));
        }

        // TODO:
        // for each row
        // make sure each attribute type matches with the catalog said it was (including null)

        // TODO:
        // NOTE: to make this step more time efficient the storage manager should probably have a
        // method to pull all primary keys as a SET
        // for each row
        // make sure primary key is unique (including the future inserts)

        // TODO:
        // if everything checks out then we insert each row using the storage manager
        System.out.println("Running insert command");

        return true;
    }
}