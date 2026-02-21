package DDLParser;

import Common.Command;
import Common.Logger;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for handling DDL parsing and proper execution
 */

public class ParserDDL {
    public static boolean parseCommand(String command) {
        boolean status = false;

        try {
            if (command.startsWith("CREATE")) {
                Command create = new CreateTable();
                Logger.log("Creating table from following command: " + command);
                status = create.run(splitRespectingQuotes(command));
            } else if (command.startsWith("DROP")) {
                Command drop = new DropTable();
                Logger.log("Dropping table from following command: " + command);
                status = drop.run(splitRespectingQuotes(command));
            } else if (command.startsWith("ALTER")) {

                Logger.log("Altering table from following command: " + command);
                String[] commandKeywords = splitRespectingQuotes(command);
                if (commandKeywords[3].equals("DROP")) {
                    Logger.log("Alter drop...");
                    Command alterDrop = new AlterTableDrop();
                    status = alterDrop.run(commandKeywords);

                } else if (commandKeywords[3].equals("ADD")) {
                    Logger.log("Alter add...");
                    Command alterAdd = new AlterTableAdd();
                    status = alterAdd.run(commandKeywords);

                }

            }

        } catch (SQLSyntaxErrorException e) {
            throw new RuntimeException(e);
        }
        if(status){
            Logger.log("Command parsed successfully");
        }
        return status;
    }

    /**
     * Function to split with respect to spaces inside quotes.
     * This is to handle default values with spaces in them for CHAR/VARCHAR values.
     *
     * @param input String we want to split by spaces/quotes
     * @return String[] containing command
     */
    private static String[] splitRespectingQuotes(String input) {
        List<String> result = new ArrayList<>();

        //Matches text inside quotes or non-whitespace sequences
        // Group 1 captures the text inside the quotes specifically
        Pattern pattern = Pattern.compile("\"([^\"]*)\"|(\\S+)");
        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            if (matcher.group(1) != null) {
                // If we matched the quoted part, add group 1 (excludes the quotes)
                result.add(matcher.group(1));
            } else {
                // Otherwise, add the unquoted word
                result.add(matcher.group(2));
            }
        }
        return result.toArray(new String[0]);


    }
}
