package DDLParser;

import Common.Command;
import Common.Logger;

import java.sql.SQLSyntaxErrorException;

/**
 * Class for handling DDL parsing and proper execution
 */

public class ParserDDL {
    public static boolean parseCommand(String command){
        boolean status = false;

        try{
            if(command.startsWith("CREATE")){
                Command create = new CreateTable();
                Logger.log("Creating table from following command: " + command);
                status = create.run(command.split("\\s+"));
            }
            else if(command.startsWith("DROP")){
                Command drop = new DropTable();
                Logger.log("Dropping table from following command: " + command);
                status = drop.run(command.split("\\s+"));
            } else if(command.startsWith("ALTER")){

                Logger.log("Altering table from following command: " + command);
                String[] commandKeywords = command.split("\\s+");
                if(commandKeywords[3].equals("DROP")){
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
        return status;
    }


}
