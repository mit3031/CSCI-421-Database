package DDLParser;

import Common.Command;
import Common.Logger;

import java.sql.SQLSyntaxErrorException;

/**
 * Command responsible for handling the drop of a table
 */
public class DropTable implements Command {
    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        if(command.length != 3){
            Logger.log("Expected 3 words in Drop Table, got " + command.length);
            throw new SQLSyntaxErrorException("Incorrect amount of words given for Dropping table!");
        }
        String tableName = command[2];
        tableName = tableName.substring(tableName.indexOf(";"));

        //TODO call storage manager to delete that table
        return true;

    }
}
