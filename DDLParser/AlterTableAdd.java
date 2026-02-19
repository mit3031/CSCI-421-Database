package DDLParser;

import Common.Command;

import java.sql.SQLSyntaxErrorException;


public class AlterTableAdd implements Command {
    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        return false;
    }
}
