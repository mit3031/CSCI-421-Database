package DMLParser;

import java.sql.SQLSyntaxErrorException;
import Common.Command;
import DMLParser.Insert;
import DMLParser.Select;
import Common.Logger;

public class ParserDML {

    public static boolean runCommand(String command)
            throws SQLSyntaxErrorException {

        // command should end with a semicolon

        int commandLength = command.length();
        command = command.toLowerCase();

        if (commandLength == 0){
            throw new SQLSyntaxErrorException("No command entered");
        }

        if (!command.endsWith(";")) {
            // throw syntax error
            throw new SQLSyntaxErrorException("Command missing semicolon");
        }

        command = command.substring(0, commandLength - 1);

        String[] commandSegments = command.split("\\s+");

        String firstWord = commandSegments[0];

        switch(firstWord){
            case "select":
                // do select parsing
                Command select = new Select();
                select.run(commandSegments);
                break;
            case "insert":
                // do create stuff
                Command insert = new Insert();
                insert.run(commandSegments);
                break;
            default:
                throw new SQLSyntaxErrorException("Invalid Command, " + firstWord + " is an unknown command");
        }


        return true;
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