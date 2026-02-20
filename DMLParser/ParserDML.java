package DMLParser;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import Common.Command;
import DMLParser.Insert;
import DMLParser.Select;
import Common.Logger;

public class ParserDML {

    /**
     * Splits a command string into tokens while preserving quoted strings.
     * Handles both single and double quotes.
     * @param command The command string to split
     * @return Array of tokens
     */
    public static String[] smartSplit(String command) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';
        
        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);
            
            if (!inQuotes && (c == '"' || c == '\'')) {
                // Start of quoted string
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                // End of quoted string
                inQuotes = false;
                tokens.add(current.toString());
                current = new StringBuilder();
            } else if (inQuotes) {
                // Inside quoted string - preserve everything including spaces
                current.append(c);
            } else if (Character.isWhitespace(c)) {
                // Outside quotes - whitespace is delimiter
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                // Normal character outside quotes
                current.append(c);
            }
        }
        
        // Add last token if exists
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        
        return tokens.toArray(new String[0]);
    }

    public static boolean runCommand(String command)
            throws SQLSyntaxErrorException {

        // command should end with a semicolon

        int commandLength = command.length();

        if (commandLength == 0){
            throw new SQLSyntaxErrorException("No command entered");
        }

        if (!command.endsWith(";")) {
            // throw syntax error
            throw new SQLSyntaxErrorException("Command missing semicolon");
        }

        command = command.substring(0, commandLength - 1);

        // For INSERT commands, we need to preserve the structure, so parse specially
        String commandLower = command.toLowerCase();
        if (commandLower.startsWith("insert")) {
            // Handle INSERT specially to preserve quoted strings with their quotes
            String[] insertTokens = parseInsertCommand(command);
            Command insert = new Insert();
            insert.run(insertTokens);
            return true;
        }

        // Use smart split for other commands (SELECT, etc.)
        String[] commandSegments = smartSplit(command);
        
        if (commandSegments.length == 0) {
            throw new SQLSyntaxErrorException("No command entered");
        }

        // Only convert first word to lowercase for keyword matching
        String firstWord = commandSegments[0].toLowerCase();

        switch(firstWord){
            case "select":
                // do select parsing
                Command select = new Select();
                select.run(commandSegments);
                break;
            case "insert":
                // Already handled above
                break;
            default:
                throw new SQLSyntaxErrorException("Invalid Command, " + firstWord + " is an unknown command");
        }


        return true;
    }
    
    /**
     * Special parser for INSERT commands that preserves quotes around string literals
     * Parses: INSERT INTO <table> VALUES (...)
     */
    private static String[] parseInsertCommand(String command) throws SQLSyntaxErrorException {
        // Find the VALUES keyword
        int valuesIndex = command.toLowerCase().indexOf("values");
        if (valuesIndex == -1) {
            throw new SQLSyntaxErrorException("INSERT statement missing VALUES keyword");
        }
        
        // Parse the part before VALUES normally
        String beforeValues = command.substring(0, valuesIndex).trim();
        String[] beforeTokens = beforeValues.split("\\s+");
        
        // Get the VALUES clause (preserve everything including quotes)
        String valuesClause = command.substring(valuesIndex + 6).trim(); // Skip "values"
        
        // Build result array: INSERT, INTO, <table>, VALUES, <values_clause>
        List<String> result = new ArrayList<>();
        for (String token : beforeTokens) {
            result.add(token);
        }
        result.add("VALUES");
        result.add(valuesClause);
        
        return result.toArray(new String[0]);
    }

    public static void main(String[] args){
        Logger.initDebug(args);
        try {
            ParserDML.runCommand("SElect * from pupppies;");
        } catch (SQLSyntaxErrorException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

}