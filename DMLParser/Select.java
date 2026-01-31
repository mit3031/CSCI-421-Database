package DMLParser;

import Common.Command;

import java.sql.SQLSyntaxErrorException;

public class Select implements Command{

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        // for right now select command is simple
        // SELECT * FROM <table>;

        if (command.length < 4){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        if (!command[0].equals("select") || !command[1].equals("*") || !command[2].equals("from")){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        String table = command[3];

        // TODO: verify that table exists


        // TODO: once implemented run the actual select command
        System.out.println("Running select * on table " + table);

        return true;
    }

}
