package DDLParser;

import Common.Command;
import Common.Logger;

import java.sql.SQLSyntaxErrorException;

/**
 * Class for handling DDL parsing and proper execution
 */
public class ParserDDL {
    public static boolean parseCommand(String command){
        try{
            if(command.startsWith("CREATE")){
                Command create = new CreateTable();
                create.run(command.split("\\s+"));
            }

        } catch (SQLSyntaxErrorException e) {
            throw new RuntimeException(e);
        }
    }


}
