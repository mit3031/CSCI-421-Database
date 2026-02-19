package DDLParser;

import Common.Command;

import java.sql.SQLSyntaxErrorException;

public class AlterTableDrop implements Command {
    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        return false;
    }
}
