package DMLParser;

import Common.Command;

import java.sql.SQLSyntaxErrorException;

public class Select implements Command{

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        // for right now select command is simple
        // SELECT * FROM <table>;


        return true;
    }

}
